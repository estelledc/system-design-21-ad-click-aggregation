# ADR 0001: PostgreSQL authority with fenced processing and immutable window publications

- Status: accepted for v0.1 design; implementation evidence pending
- Date: 2026-08-19

## Context

The chapter's component diagram does not resolve the hardest correctness boundary: a retrying event, worker state, input progress,
watermark, final result, late correction, and query page must agree about which event contributed. “Exactly once” is otherwise an
ambiguous label that can refer to producer retry, queue delivery, managed state, sink visibility, or an external financial effect.

The lab needs real concurrency and restart evidence in a small public CI environment. It must also preserve the distinction between
aggregate visibility and fraud/attribution/billing outcomes.

## Decision

Use PostgreSQL 17 as one v0.1 transactional authority around:

- two fixed logical sources/partitions;
- source epoch/sequence plus never-reset batch revision and stable event ID/payload digest;
- explicit worker assignment generations;
- one transaction for event disposition, provisional count, process receipt, checkpoint, and processed watermark;
- a global watermark equal to the required partition minimum;
- a two-minute allowed-lateness publication policy, labelled heuristic and not absolute completeness;
- append-only full-snapshot publication revisions;
- one explicit idempotent `+1` correction for each stored late event;
- query results materialized under one publication revision;
- an event-time retention floor that rejects unsafe old replays before purging identity.

Use short `READ COMMITTED` transactions with explicit row locks and database uniqueness constraints. The correctness proof depends
on locked authority rows and constraints, not on a read-modify-write sequence outside the database.

## Meaning of exactly once

The only positive claim is: for one aggregate definition, one canonical owned event creates at most one committed base disposition,
and its disposition/count update commits atomically with the partition's next batch checkpoint. Code may physically execute more
than once. Late correction is a separate unique effect and publication revision.

This does not prove one delivery, source honesty, Kafka/Flink semantics, external sink idempotence, genuine clicks, attribution,
billing, charge, or human/external acceptance.

## Alternatives considered

### In-memory event loop and files

Rejected because it cannot independently prove concurrent unique enforcement, transactional rollback, or database restart
behavior. A file snapshot would turn this case into another custom WAL implementation instead of isolating window semantics.

### Kafka plus Flink plus ClickHouse/Cassandra

Deferred. It would be more representative of the chapter's throughput architecture, but it introduces multiple connector,
checkpoint, transaction, watermark, schema, and deployment contracts. A green stack would still not prove stable application-level
event identity or a billing effect. The lab first needs a compact executable oracle.

### PostgreSQL mutable aggregate rows only

Rejected because a query could mix before/after correction state, response loss would make finalization ambiguous, and historical
reports could change invisibly. Publication revisions intentionally duplicate small lab data to make snapshot identity explicit.

### Processing-time windows

Rejected because queue/worker delay would change the event's business bucket. Event-time remains untrusted input, so validation,
source-watermark heuristics, late ledger, and correction evidence are still required.

### Wall-clock `now - delay` finalization

Rejected because wall clock does not prove source or partition progress. The lab advances only from watermarks attached to processed
source batch prefixes and takes the required partition minimum. A quiet partition advances explicitly rather than through timeout.

## Consequences

Positive:

- the core crash windows become one-database transactions with inspectable constraints;
- stable receipts and immutable revisions are easy to test under response loss and process death;
- the source chapter's watermark/exactly-once ambiguities become observable API fields;
- synthetic data and one dependency keep public CI bounded.

Negative:

- one database is not the one-billion/day architecture and hides queue/network/component failover;
- two fixed partitions do not prove repartitioning, dynamic idleness, or distributed checkpoint alignment;
- full-snapshot publication has intentionally poor write amplification;
- source-declared watermarks can be wrong, so finalization is a policy fact and late correction remains necessary;
- logical retention proves rejection semantics, not object-store archive, long replay, or legal retention.

## Revisit triggers

Revisit only if a later requirement needs independently failing services, protocol compatibility, dynamic repartitioning, arbitrary
windows/dimensions, negative corrections, durable raw replay, or production-scale OLAP. Any migration must preserve stable event
identity, generation fencing, state/checkpoint atomicity, watermark provenance, immutable publications, correction lineage, and
query snapshot receipts before claiming equivalence.
