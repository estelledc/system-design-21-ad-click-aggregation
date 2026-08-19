# Architecture

## Business view

The lab answers a narrow question: given two retrying synthetic sources, which deduplicated events contributed to which minute
window at one immutable publication revision? A separate correction may add a previously late event. Nothing in that chain decides
whether a click was human, attributable, billable, charged, or accepted by an advertiser.

```text
source 0 batches ----\
                      > durable event/batch ledger -- fenced workers -- provisional minute counts
source 1 batches ----/              |                       |                    |
                                    |                 atomic checkpoint          |
                                    |                       |                    v
                                    +-- duplicate/conflict   +-- min processed watermark
                                                                                 |
late after published cutoff --> late ledger --> explicit correction --------> immutable publications
                                                                                 |
                                                                       materialized query receipt
```

PostgreSQL 17 is the only authority in v0.1. The arrows are logical components and transaction boundaries inside one process and
database, not independently deployed services.

## State model

### Ingest authority

- `sources` owns a fixed partition, current epoch, next sequence, monotonic batch revision, next input offset, and latest declared
  watermark.
- `batches` binds source/epoch/sequence to one digest, source batch revision, watermark, counts, and durable result.
- `events` owns the first canonical occurrence of an event ID and its immutable event-time/dimension digest.
- `batch_events` records original ordering and whether this batch owns the event or merely references an exact duplicate.

An exact batch retry is checked before current-epoch/floor validation so response-loss recovery still works after an epoch rotation
or retention advance. A new old event is checked against the retention floor before dedup lookup and is rejected explicitly.

### Processing authority

- Each partition has one current assignment generation and worker.
- A worker requests the next source batch revision, never an arbitrary offset range.
- One transaction locks the current assignment and global publication state, classifies owned events, writes unique dispositions,
  updates provisional partials, stores the process receipt, and advances checkpoint/watermark.
- An event at or behind the latest published cutoff becomes `late_unapplied`; otherwise it becomes `counted` and increments exactly
  one base partial.

This transaction is the v0.1 meaning of exactly-once state contribution. Physical code may run again after a crash; the committed
effect cannot occur twice.

### Watermark and publication authority

The source watermark is a monotonic, source-declared heuristic. It is not derived from wall clock and is not advertised as proof
that no earlier event can arrive. Processing binds that watermark to a committed batch prefix. The global watermark is:

```text
min(partition_0.processed_watermark, partition_1.processed_watermark)
```

No timeout silently removes an idle partition. A quiet source advances only with an explicit zero-event batch. This trades
availability for an inspectable completeness policy.

With a two-minute allowed-lateness policy, the publisher rounds `global_watermark - 2 minutes` down to a minute boundary. Every
window ending at or before that boundary is `finalizedByPolicy`. A publication stores that cutoff and a complete immutable snapshot
of all visible base counts plus corrections through its correction generation. Numeric zero is inferred only for a published
window with no matching row; an unpublished window returns `null`.

### Correction authority

A normal event processed behind the current published cutoff does not mutate the old count. It enters the late ledger. An operator
may apply exactly one `+1` correction for that event under the expected latest publication. The transaction records the correction,
increments correction generation, creates a new publication, rematerializes rows, and stores the stable receipt. The old revision
remains queryable.

Negative corrections, fraud invalidations, attribution changes, and financial adjustments are outside v0.1 because their source
of truth is not this event ledger.

### Query authority

A query selects a named publication or the latest one inside its creation transaction, computes a bounded result, and stores the
whole result JSON. Pages read only that materialized receipt. Later finalization, correction, retention, or source activity cannot
change an existing query.

Count queries expose every requested minute and distinguish `finalized`, `corrected`, and `not_published`. Top-N queries reject a
range that is not fully published. Ties are ordered by normalized `adId` so pagination is deterministic.

## Atomic boundaries and crash outcomes

| Crash window | Recovery result |
|---|---|
| after durable batch commit, before HTTP response | same epoch/sequence/digest returns original batch revision and counts |
| during changed-duplicate batch | whole transaction rolls back; no source sequence, event, or offset advances |
| after event disposition/partial, before checkpoint | impossible as a committed split state; one transaction contains both |
| after processing commit, before response | same process request returns receipt; different next revision cannot reapply events |
| old worker after takeover | generation mismatch fences new contribution/checkpoint |
| one partition advances, one stalls | minimum watermark stalls publication; state reports lag |
| after publication commit, before response | same mutation key returns original revision; rows are append-only |
| event arrives behind published cutoff | stored as late; old revision unchanged |
| after correction commit, before response | same correction identity returns original new revision; event has one correction |
| after query creation, before page response | query pages remain bound to stored result and publication |
| after retention floor moves | new event older than floor is rejected; old batch receipt can still replay |

## Why one PostgreSQL authority

The target invariant crosses dedup identity, worker fencing, mutable provisional state, input progress, immutable publication, and
correction receipt. PostgreSQL unique constraints, row locks, and transactions let the lab make that boundary executable in public
CI. An in-memory model cannot prove restart/concurrency behavior. A full Kafka/Flink/OLAP stack would test products and connectors
before the semantic contract is stable and still would not automatically bind an external billing effect.

This choice sacrifices scale realism and independent component failure. It is a correctness oracle and learning slice, not the
chapter's proposed Facebook/Google-scale deployment.

## Capacity and hotspot implications not implemented

- partition count is fixed at two; no reshard/handoff algorithm is claimed;
- one PostgreSQL writer path can serialize hot state and cannot represent the stated billion/day envelope;
- pre-aggregated ad/region/device tuples are bounded but still demonstrate multiplicative cardinality;
- top-N scans a bounded publication snapshot rather than maintaining distributed local/global heaps;
- full-snapshot publication is intentionally expensive but makes revision identity obvious;
- raw retention is a logical event-time floor and local purge, not object-storage lifecycle or replay infrastructure.

Those are explicit scale gaps, not hidden TODOs behind a production-sounding diagram.
