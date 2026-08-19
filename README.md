# Ad Click Event Aggregation Lab

This clean-room system-design practice starts with one question: when click events are duplicated, reordered, delayed across a
window boundary, or replayed after a worker crash, what immutable event and window receipts let a query explain its count without
turning “aggregate visible” into an invented claim that a click was valid, attributable, billable, or charged?

The prompt title is “Ad Click Event Aggregation.” This repository does not copy an analytics product, source chapter, protocol,
diagram, benchmark, event stream, ad data, or proprietary behavior. The initial contract is frozen from the title alone before
reading the fixed secondary chapter.

## Current phase

The closed-book contract and source-calibrated design are frozen in separate commits. The selected v0.1 slice uses PostgreSQL 17 as
one transactional authority around two logical input partitions, stable event/batch identities, fenced workers, atomic
contribution/checkpoint, source-declared heuristic watermarks, immutable publication revisions, explicit late corrections, and
snapshot-bound queries. Implementation, executable validation, benchmark, public remote, and CI are still pending.

This is deliberately not a Kafka, Flink, Beam, Cassandra, ClickHouse, Druid, billing, or fraud implementation. The lab models a
small set of their surrounding correctness questions without claiming protocol compatibility or production scale.

## Read first

- [Closed-book contract](docs/closed-book-contract.md)
- [Source comparison and evidence log](docs/research-log.md)
- [Requirements](docs/requirements.md)
- [Architecture](docs/architecture.md)
- [API contract](docs/api.md)
- [ADR 0001](docs/adr/0001-postgres-fenced-window-publications.md)
- [Security policy](SECURITY.md)

## Evidence boundary

SDK emission, gateway validation, durable event receipt, aggregate contribution, window publication, query visibility, fraud
classification, attribution, billable decision, ledger entry, charge, advertiser report, and business acceptance are different
facts. This lab will name and test only the boundaries it reaches. It will abstain from real click validity, unique-human counts,
conversion attribution, billing accuracy, revenue, fraud prevention, production capacity, SLA, or external acceptance without
matching evidence.

## License

MIT. Third-party study material and real advertising/event data are not included.
