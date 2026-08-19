# Ad Click Event Aggregation Lab

This clean-room system-design practice starts with one question: when click events are duplicated, reordered, delayed across a
window boundary, or replayed after a worker crash, what immutable event and window receipts let a query explain its count without
turning “aggregate visible” into an invented claim that a click was valid, attributable, billable, or charged?

The prompt title is “Ad Click Event Aggregation.” This repository does not copy an analytics product, source chapter, protocol,
diagram, benchmark, event stream, ad data, or proprietary behavior. The initial contract is frozen from the title alone before
reading the fixed secondary chapter.

## What is implemented

The closed-book contract and source-calibrated design are frozen in separate commits. The v0.1 slice uses PostgreSQL 17 as one
transactional authority around:

- exactly two logical source partitions with stable source epoch/sequence, never-reset batch revision, input offset, and batch
  receipt;
- a global synthetic event ID and immutable event-time/dimension digest, so an application resend at another log position is still
  one event;
- explicit worker assignment generations and one transaction for event disposition, provisional count, process receipt,
  checkpoint, and processed watermark;
- one-minute UTC `[start, end)` windows, a source-declared heuristic watermark, the required partition minimum, and a two-minute
  allowed-lateness publication policy;
- append-only full-snapshot publication revisions, stored late events, explicit idempotent `+1` corrections, and materialized query
  receipts/pages;
- an event-time retention floor that rejects unsafe old replays before removing the event ledger.

The implementation, pure tests, real PostgreSQL tests, four-boundary true-process crash smoke, and bounded benchmark pass locally.
The first public implementation run, [GitHub Actions 32201204205](https://github.com/estelledc/system-design-21-ad-click-aggregation/actions/runs/32201204205),
also passed on Node 22/24/26 with PostgreSQL 17.11. Each job reported 18 pure tests and 8 PostgreSQL tests with zero
failures, skips, or todos. This is a reproducibility receipt for the named synthetic boundary, not external product acceptance.

This is deliberately not a Kafka, Flink, Beam, Cassandra, ClickHouse, Druid, billing, or fraud implementation. The lab models a
small set of their surrounding correctness questions without claiming protocol compatibility or production scale.

## One count, six different facts

```text
durable source batch
  -> canonical event owner
  -> counted or late disposition + checkpoint
  -> immutable publication revision
  -> materialized query receipt
  != fraud-valid click
  != attributed or billable quantity
  != ledger entry, charge, settlement, or advertiser acceptance
```

The positive “exactly once” claim stops at one event affecting this lab's committed base aggregation state at most once. Physical
processing may run repeatedly after a crash. A late correction is another explicit unique effect and another publication.

## Run locally

Requirements: Node.js 22 or newer and PostgreSQL 17.

```sh
npm ci --ignore-scripts
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/ad_click_aggregation'
export AD_CLICK_API_TOKEN='replace-with-a-local-token-at-least-16-characters'
npm start
```

Run the gates against an empty disposable database:

```sh
npm run check
npm run test:infra
npm run smoke:infra
npm run benchmark:infra
```

`test:infra`, `smoke:infra`, and `benchmark:infra` fail when `DATABASE_URL` is absent; they never convert missing infrastructure
into a skipped success. The smoke resets the named database and is destructive to data in that disposable database.

## Evidence produced locally

- Pure model/contract/HTTP tests cover canonical identity, exact shapes, half-open boundaries, generated timestamps, watermark
  cutoffs, zero versus unpublished, corrected status, top-N tie order, pagination, and conservation.
- PostgreSQL tests cover durable retry, whole-batch conflict rollback, concurrent cross-partition duplicate ownership, atomic
  contribution/checkpoint, takeover fencing, minimum watermark, immutable publications, explicit correction, stable query pages,
  retention horizon, and deterministic top-N.
- The smoke kills four separate API processes after ingest, process/checkpoint, publication, and correction commits. Exact retries
  recover revisions `1`, `1`, `1`, and `2`; publication 1 remains count `1` while corrected publication 2 is count `2`.
- The benchmark fixture is fixed at 2 partitions, 34 batches, 2,048 submitted events, 2,018 canonical events, 30 duplicate
  references, 16 minute windows, 32 ads, 64 queries, one late event, and one correction. Results are runtime observations only.

## Read first

- [Closed-book contract](docs/closed-book-contract.md)
- [Source comparison and evidence log](docs/research-log.md)
- [Requirements](docs/requirements.md)
- [Architecture](docs/architecture.md)
- [API contract](docs/api.md)
- [ADR 0001](docs/adr/0001-postgres-fenced-window-publications.md)
- [Operations](docs/operations.md)
- [Threat model](docs/threat-model.md)
- [Verification ledger](docs/verification.md)
- [Security policy](SECURITY.md)

## Evidence boundary

SDK emission, gateway validation, durable event receipt, aggregate contribution, window publication, query visibility, fraud
classification, attribution, billable decision, ledger entry, charge, advertiser report, and business acceptance are different
facts. This lab will name and test only the boundaries it reaches. It will abstain from real click validity, unique-human counts,
conversion attribution, billing accuracy, revenue, fraud prevention, production capacity, SLA, or external acceptance without
matching evidence.

## Deliberate limits

This is deliberately not a Kafka, Flink, Beam, Cassandra, ClickHouse, Druid, billing, or fraud implementation. It has one database,
one tenant, two fixed partitions, three bounded dimensions, one minute window, positive corrections only, no dynamic resharding,
no independently failing sink, no production backup/failover, and no real advertising data. A green test or CI run proves only the
named synthetic boundary.

## License

MIT. Third-party study material and real advertising/event data are not included.
