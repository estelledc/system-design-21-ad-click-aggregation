# Closed-book contract: replayable click contributions and versioned windows

## Reading boundary

This contract was written from the case title alone, before reading the fixed `system-design-notes` chapter. “Ad Click Event
Aggregation” names a problem family, not one product or billing contract. SDK collection, HTTP/log/queue ingestion, stream
processing, event-time windows, watermark algorithms, deduplication, dimension enrichment, OLAP storage, materialized views,
late-data correction, fraud filtering, attribution, reporting, and charging are hypotheses for this lab—not facts about Kafka,
Flink, Spark, Beam, ClickHouse, Druid, Pinot, BigQuery, ad networks, or any other system. Later research must record confirmations,
conflicts, omissions, and changes instead of silently rewriting this baseline.

## Users and core behavior

### Synthetic click producer

1. An authenticated synthetic producer publishes a bounded batch under a stable producer epoch and batch sequence.
2. Each click carries a globally stable event ID inside the tenant, canonical event time, bounded ad/campaign dimensions, and no
   real person/device/tracking identity.
3. Retrying the same event ID or batch identity with the same canonical intent returns the original receipt. Changed intent
   conflicts; stale epochs and sequence gaps fail explicitly.
4. A successful response names whether bytes were validated, durably recorded, or included in a published aggregate revision. It
   does not imply a genuine click, unique person, valid attribution, billability, charge, report, or advertiser acceptance.

### Analytics consumer

1. A caller requests bounded counts for explicit dimensions, half-open event-time windows, and a named publication snapshot.
2. The result states whether each window is provisional, finalized, corrected, partial, missing, or beyond retained history.
3. Pagination/chunking under one stable query identity cannot mix publication revisions or dimension-schema generations.
4. Raw event count, deduplicated event count, filtered count, attributed count, billable count, and money are different measures;
   v0.1 must not rename one as another.

### Aggregation operator

1. The operator owns partition assignment generations, worker leases/fences, event-time lateness policy, watermarks, window state,
   publication revisions, correction generations, retention, and replay budgets under strong preconditions.
2. A worker can replay a durable input range without double-contributing an event. A superseded worker cannot checkpoint or
   publish after takeover.
3. Ordinary health exposes lag, watermark age, duplicate/conflict/late counts, unpublished windows, correction backlog, and
   storage state without payload identities or high-cardinality dimensions.

### Fraud, attribution, billing, or finance consumer

1. This lab may expose an aggregate receipt as an input candidate only.
2. Fraud decision, valid-human click, impression/click attribution, contract terms, price, currency, tax, invoice, ledger entry,
   charge, settlement, reconciliation, and dispute are separate systems and receipts.
3. No aggregate response or database row is called “revenue,” “cost,” “billable,” “paid,” or “accurate billing” without those
   independent contracts and evidence.

## Non-goals for v0.1

- real advertisers, campaigns, ads, users, devices, IPs, cookies, mobile identifiers, tracking URLs, clickstream, conversions,
  pricing, invoices, money, fraud labels, or production traffic;
- browser/mobile SDK behavior, redirects, impression collection, conversion attribution, identity graphs, bot/fraud detection,
  privacy consent, frequency caps, auction, ranking, budget pacing, or ad serving;
- protocol compatibility with Kafka/Flink/Beam/Spark/ClickHouse/Druid/Pinot/BigQuery or any vendor;
- globally distributed ingest/query, multi-region disaster recovery, consensus implementation, arbitrary repartitioning, custom
  stream engine/OLAP database, production backup/restore, deployment, SLA, capacity, cost, or business claims;
- unbounded event age, dimensions, batch size, partitions, windows, query range/results, corrections, replay, or retention;
- proving client receipt, click validity, unique person, attribution, billability, ledger correctness, charge, report delivery,
  advertiser acknowledgement, revenue, or external acceptance.

## Hypothetical scale envelope

These numbers select failure modes; they are not sourced requirements:

- one billion synthetic click events/day is about 11,574 events/s average; a 10× burst is about 115,741 events/s;
- at a hypothetical 240 canonical bytes/event, raw input is about 240 GB/day before queue framing, indexes, metadata, WAL,
  deduplication state, aggregates, compression, and replicas;
- three durable copies of that raw component alone are about 720 GB/day or 5.04 TB for seven days;
- one-minute windows produce 1,440 time buckets/day per active dimension tuple; five million ad × region × device combinations
  would be billions of sparse groups if dimensions were expanded without a hard budget;
- a seven-day event-ID deduplication horizon at one billion events/day contains seven billion identities before representation and
  compaction choices;
- 0.1% of events arriving after finalization is still one million late events/day requiring explicit reject or correction work.

The runnable lab will use a small synthetic fixture, report exact event/window/dimension/replay/correction counts, and never
extrapolate it to an advertising platform.

## Candidate data and state model

Event and batch identity:

```text
tenant + event_id -> immutable canonical click intent digest
producer + epoch + batch_sequence -> immutable ordered batch digest and durable receipt
event_time + window_size -> half-open UTC window [start, end)
```

Ingestion and contribution:

```text
received -> validated -> durable event ledger -> partition-visible input position
                              | exact duplicate -> original event receipt
                              \ changed event ID -> conflict

(assignment generation, input position, event ID)
    -> claimed -> contribution recorded -> checkpointed
```

Window publication:

```text
open/provisional -> watermark passes end + lateness -> finalized revision
       |                                      |
       +-- bounded late event ----------------+-> explicit correction generation or late ledger

partition watermarks -> global/query watermark -> immutable publication manifest
```

## Core invariants

1. **Stable event identity.** One tenant/event ID binds one canonical event time and dimension payload. Exact retry replays; changed
   payload conflicts. Event ID is not derived only from mutable dimensions or arrival time.
2. **Stable batch identity.** Producer epoch/sequence and canonical batch digest bind one immutable receipt. Stale epochs, sequence
   gaps, and changed retries cannot silently append.
3. **Event time is explicit.** Event time, producer time, receive time, durable time, processing time, watermark time, publication
   time, query time, fraud/billing time, and report time are separate clocks.
4. **Windows are canonical.** UTC half-open `[start, end)` boundaries, size, alignment, timezone, and schema generation are part of
   aggregate identity. An event exactly at `end` belongs to the next window.
5. **One event, at most one contribution per aggregate definition.** Duplicate delivery, worker retry, replay, and response loss
   cannot increment the same measure twice.
6. **Changed duplicate is not another click.** Reusing an event ID with another ad/campaign/time/dimension payload is a conflict or
   audited correction, never a second count.
7. **Dimension meaning is frozen.** Aggregation binds the event's validated dimension snapshot or an explicit enrichment version;
   joining current campaign metadata later cannot silently rewrite old windows.
8. **Partition mapping is versioned.** Routing/assignment generation is carried through ingest, worker claim, checkpoint, watermark,
   and publication. Repartitioning cannot create overlap/gap without an explicit handoff boundary.
9. **Workers are fenced.** A superseded generation/lease may leave uncommitted work but cannot record a current contribution,
   checkpoint, watermark, or publication.
10. **Checkpoint does not outrun contribution.** Durable event contribution and input progress commit atomically or are joined by a
    replayable transaction/outbox protocol. Crash cannot skip an uncounted event.
11. **Watermark names coverage, not wall clock.** It advances only over a proven input prefix/partition policy. Idle or failed
    sources need explicit rules; `now - delay` alone is not completeness evidence.
12. **Global completeness is the required partition minimum.** A fast shard cannot finalize a cross-shard window while a required
    shard remains behind, unless the result is explicitly partial.
13. **Provisional and finalized are different facts.** A provisional count may change. Finalization freezes a publication revision;
    later mutation requires an explicit correction generation.
14. **Late data policy is deterministic.** Before watermark, a late event can update provisional state. After finalization it is
    rejected/quarantined or creates a versioned correction; it never silently mutates a previously receipted result.
15. **Corrections are algebraic and idempotent.** Positive/negative deltas bind the original event/window/definition and one
    correction ID. Replaying a correction cannot double-adjust.
16. **One query, one publication snapshot.** Pagination, multiple windows, and retries under one query identity cannot mix manifest,
    dimension, or correction generations. Changed intent conflicts.
17. **Missing/partial/retained-away are not zero.** No matching events, missing input partitions, late backlog, failed enrichment,
    retained event IDs, and an actual zero count remain distinguishable.
18. **Dedup retention is part of correctness.** Dropping event-ID history creates a named replay horizon. Events older than that
    cannot be safely labeled unique without another source of truth.
19. **Replay is bounded and isolated.** Backfill/recomputation has a stable job/generation, input range, output namespace, resource
    budget, and atomic promotion. It cannot race live output under the same mutable keys.
20. **Backpressure is visible.** Ingest, durable log, dedup, worker, state store, publication, query, correction, and retention
    limits reject/throttle with typed receipts; no silent drop is labeled success.
21. **Counts are not money.** Even a finalized deduplicated synthetic click count proves neither fraud validity, attribution,
    contract eligibility, billable quantity, price, ledger entry, charge, settlement, nor revenue.
22. **Evidence is privacy-bounded.** Ordinary logs/metrics omit event/ad/campaign/user/device IDs, dimension values, query filters,
    raw payloads, digests, auth values, correction contents, pricing, and report recipients.
23. **Self-monitoring is not self-proof.** Lag/watermark alarms need an independent path/receipt; a query served by the same stalled
    aggregation pipeline cannot prove completeness.

## Initial API and event sketch

Authenticated ingest routes:

- `POST /v1/producers` and `POST /v1/producers/{id}/epochs` for synthetic producer authority;
- `POST /v1/click-batches` with epoch, sequence, event IDs, event times, dimensions, and a required durability receipt;
- `GET /v1/ingest-receipts/{id}` for response-loss recovery without payload logging.

Aggregation/operator routes:

- `POST /v1/assignments/generations` with expected generation and explicit partition handoff;
- `POST /v1/workers/{id}/claim` plus fenced contribution/checkpoint completion;
- `POST /v1/windows/{definition}/{start}/finalize` with required watermark/publication preconditions;
- `POST /v1/corrections` with stable correction identity and expected publication revision;
- `GET /v1/state` for positions, watermarks, lag, duplicate/conflict/late/backlog counts without payload identities.

Query routes:

- `POST /v1/aggregate-queries` with definition/dimensions/window range/work caps and stable query key;
- `GET /v1/aggregate-queries/{id}/pages/{cursor}` bound to one publication manifest/correction generation.

Exact event schema, batch ordering, partitioning, window sizes, lateness/watermark algorithm, dimension/enrichment model, correction
policy, retention, query form, auth, storage, and error vocabulary remain hypotheses until primary sources are reviewed.

## Failure matrix

| Failure window | Required result |
|---|---|
| producer loses response after durable batch | exact epoch/sequence/digest returns original receipt; changed batch conflicts |
| same event appears in two batches/partitions | one canonical event and one contribution; duplicate receipt names prior result |
| same event ID arrives with changed dimensions/time | conflict or explicit correction; never a second implicit click |
| worker dies after contribution before checkpoint | replay finds the contribution and advances once; count cannot double |
| worker dies after checkpoint before response | stable completion receipt replays; checkpoint cannot create a second contribution |
| old worker finishes after assignment takeover | generation/lease fence rejects its contribution/checkpoint/watermark/publication |
| partition stalls while others advance | cross-partition watermark waits or result is explicitly partial; no false finalization |
| event arrives before watermark but out of order | provisional window updates once under event identity |
| event arrives after finalized window | typed late/reject/quarantine or correction generation; old receipt remains immutable |
| correction process dies midway | unpromoted generation remains invisible; exact recovery reuses or discards it safely |
| query page response is lost | exact query/cursor returns same manifest page; changed intent conflicts |
| dedup state expires before old replay | explicit beyond-dedup-horizon result; no claim of uniqueness |
| retention removes raw events under a report | retained publication stays bound to its lineage or returns explicit evidence gap |
| cardinality/window explosion crosses budget | typed rejection/throttle before unbounded state allocation |
| count is consumed by billing | API/receipt still says aggregate only; no charge/revenue claim without an independent ledger |

## Required executable evidence before v0.1 completion

1. A clean-room README, source comparison, requirements, architecture, API/events, operations, threat model, and at least one ADR.
2. Exact event/batch/window/assignment/query/correction validation and stable-identity/error-vocabulary tests.
3. Generated/property tests for canonical windows, boundary timestamps, duplicate/reorder/replay, contribution conservation,
   watermark monotonicity, finalized immutability, correction algebra, and snapshot-stable queries.
4. Real persistence/concurrency tests for durable retry, changed duplicate rollback, worker fencing, atomic contribution/checkpoint,
   partition watermark join, finalization/publication, late correction, retention horizon, and restart.
5. A true-process crash/restart smoke covering ingest response loss, contribution/checkpoint ambiguity, stale takeover, finalize/
   publish response loss, late correction recovery, and no fraud/billing/external claim.
6. A bounded benchmark with exact events/dimensions/partitions/windows/duplicates/late/corrections/query fixture, runtime/service
   versions, exclusions, and raw observations—without production or revenue extrapolation.
7. Node 22/24/26 public CI with pinned actions/services, dependency audit, zero skipped infrastructure tests, and exact commit/run
   receipts.
8. Privacy/evidence scans that reject synthetic identifier/value/pricing sentinels and any claim of real click validity,
   attribution, billability, charge, revenue, SLA, or external acceptance without matching evidence.

## Initial design choices to challenge after source review

- Does the chapter define click identity and immutable payload, or assume transport offsets make duplicates harmless?
- Is event time distinct from processing time, and are windows half-open with a precise boundary/timezone?
- What proves a watermark covers every required partition, including idle/failing sources?
- Does “exactly once” refer to queue delivery, state-store contribution, checkpoint, published count, billing, or an external effect?
- Are aggregation state and input checkpoint atomic, or can a crash double-count/skip?
- Can late events mutate finalized reports, and if so how are versions/corrections/retractions represented?
- Are dimensions captured at event time or joined from mutable campaign metadata?
- Does deduplication retention exceed every supported replay/backfill horizon?
- Are hot keys, sparse dimension explosion, backpressure, replay isolation, and retention explicitly bounded?
- Are fraud, attribution, billing, privacy, consent, and financial ledger outcomes kept outside raw click aggregation evidence?
