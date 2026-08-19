# Requirements and acceptance contract

## Users and supported behavior

### Synthetic source

- Register one bounded source for each of exactly two logical partitions.
- Rotate a source epoch under an expected-epoch precondition; batch sequence restarts at zero while monotonic source batch revision
  and watermark history continue.
- Append zero through 64 synthetic events with a stable source epoch/sequence, immutable batch digest, and monotonic
  source-declared watermark.
- Replay an exact batch as the original durable receipt; reject changed batch identity, stale epoch, sequence gap, changed event
  identity, invalid boundary, or event time before the logical dedup floor.

### Aggregation worker

- Acquire one partition through an explicit assignment generation.
- Process exactly the next durable source batch. A commit atomically records each owned event's disposition, increments any
  provisional count, advances the batch checkpoint, and advances that partition's processed watermark.
- Retry an exact committed process request as its original receipt. A stale generation cannot contribute or checkpoint after
  takeover.

### Publisher and correction operator

- Publish only through the minimum processed watermark across both required partitions.
- Freeze minute windows whose end plus two-minute allowed-lateness policy is not later than that global watermark.
- Keep every publication revision immutable. A late event behind an already published cutoff is recorded but not silently added.
- Apply one explicit `+1` correction for one stored late event under an expected-publication precondition; create a new correction
  generation and publication revision while preserving the old result.
- Advance the logical dedup floor only after all current batches are processed, affected windows are published, and no unapplied
  late event lies before the boundary.

### Analytics caller

- Create a bounded count or top-N query under a stable request key and one publication revision.
- Count queries return minute buckets with `finalized`, `corrected`, or `not_published`; unpublished is `null`, not zero.
- Top-N queries require a fully published range, use deterministic count-descending/ad-ID-ascending ordering, and stay within a
  fixed result cap.
- Every query receipt names the publication, correction generation, watermark kind, finalized cutoff, measure, raw-evidence
  retention state, and explicit false flags for fraud validity, attribution, billing, charge, and external acceptance.
- Query pages are materialized once; a retry or later publication cannot change them.

## Fixed v0.1 policy

| Item | Contract |
|---|---|
| Tenant | one synthetic tenant, not externally supplied |
| Input partitions | exactly `0` and `1`; one registered source each |
| Window | one-minute UTC tumbling window, `[start, end)` |
| Allowed lateness | two minutes after window end for publication eligibility |
| Dimensions | `adId`, `region`, `device`; all bounded identifiers |
| Measure | deduplicated synthetic event count, integer `+1` only |
| Source watermark | monotonic, source-declared heuristic; not completeness proof |
| Global watermark | minimum processed watermark over both partitions |
| Late policy | store as unapplied late event, then explicit `+1` correction or leave pending |
| Query range | at most 60 minute windows |
| Query page | at most 20 rows; materialized JSON receipt |
| Top N | 1 through 20 |
| Batch | zero through 64 events; zero-event batch may advance a quiet partition's watermark explicitly |
| Retention | event-time floor; old event IDs are rejected after logical purge |

## Core invariants selected for execution

1. One `eventId` binds one canonical event time and dimension payload. Exact duplicate is not a second contribution; changed
   duplicate rolls back the entire new batch.
2. One source epoch/sequence binds one canonical batch and original receipt. Source batch revision never resets.
3. Event windows are canonical UTC half-open minutes. The exact end timestamp belongs to the next window.
4. Only the first durable event owner can reach a disposition; duplicate references in later batches do not.
5. One aggregate definition/event has at most one base disposition: `counted` or `late_unapplied`.
6. Worker generation, next batch revision, event dispositions, partial counts, checkpoint, and processed watermark are checked and
   committed in one PostgreSQL transaction.
7. A stale worker can observe an already committed receipt but cannot create current state after assignment takeover.
8. A publication uses the minimum of both processed watermarks and a fixed allowed-lateness policy. It calls that cutoff
   `finalizedByPolicy`, never absolute event completeness.
9. Publication rows are append-only by revision. Corrections create another revision and generation.
10. One query receipt binds one publication and materialized result. Pages never read mutable live partials.
11. Advancing retention first creates a rejection floor; an old replay is `beyond_dedup_horizon`, not a newly accepted click.
12. Aggregate, fraud, attribution, billable quantity, ledger entry, charge, settlement, and advertiser acceptance remain different
    receipts.

## Hypothetical scale envelope

The title's one-billion-events/day prompt gives roughly 11,574 events/s average. A hypothetical 10× burst gives about 115,741/s.
At 240 canonical bytes/event, raw input alone is about 240 GB/day; three copies are 720 GB/day. Seven days of event identity means
seven billion dedup keys before representation overhead. A 0.1% post-finalization rate still yields one million correction
candidates/day. Five million active ad/region/device tuples across 1,440 minute buckets/day would create billions of sparse groups.

These calculations select failure modes. The lab benchmark uses a small exact fixture and cannot validate those rates, storage
sizes, costs, SLAs, or production architecture.

## Non-goals

- real click collection, redirects, users/devices/IPs/cookies, consent, attribution, fraud, auction, pacing, pricing, money, billing,
  finance, reports, or advertiser workflows;
- Kafka/Flink/Beam/Spark/Cassandra/ClickHouse/Druid/Pinot/BigQuery compatibility;
- arbitrary windows, schemas, repartitioning, sessions, negative corrections, mutable enrichment, unbounded top-N, or ad-hoc SQL;
- multi-tenant isolation, multi-region durability, consensus, database failover, backups, production deployment, capacity, or SLA;
- treating a synthetic watermark, local database transaction, green CI run, or API response as external business acceptance.

## Completion evidence

- pure tests for validation, canonical identity/windowing, conservation, boundaries, watermark cutoffs, correction algebra, and
  deterministic queries;
- PostgreSQL 17 tests for retry/conflict rollback, concurrent duplicate ingest, assignment fencing, atomic processing/checkpoint,
  watermark minimum, immutable publications, late correction, retention, and query snapshot stability;
- a true-process `SIGKILL` smoke at ingest commit, process/checkpoint commit, publication commit, and correction commit;
- a fixed synthetic benchmark with exact event/batch/partition/window/duplicate/late/correction/query counts and no extrapolation;
- Node 22/24/26 public CI against pinned PostgreSQL 17.11, no skipped infrastructure tests, dependency audit, privacy/evidence
  scan, exact commit/run receipts, and explicit unproved boundaries.
