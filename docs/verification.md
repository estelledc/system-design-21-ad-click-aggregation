# Verification ledger

## Current status

Local implementation gates pass on Node `v26.7.0` and PostgreSQL `17.11 (Homebrew)`. Public repository, clean-clone reproduction,
Node 22/24/26 GitHub Actions, and external acceptance are pending. This page will not call the repository complete until the public
receipts exist.

## Local correctness receipts

| Gate | Current local result | Proves | Does not prove |
|---|---|---|---|
| `npm test` | 18 pure tests, 0 failed/skipped/todo | validation, canonical identity/windowing, generated boundary properties, watermark cutoff, query semantics, HTTP auth/routing | persistence, concurrency, restart |
| `npm run test:infra` | 8 PostgreSQL tests, 0 failed/skipped/todo | durable retry, changed rollback, concurrent duplicate ownership, atomic checkpoint, fencing, minimum watermark, revisions/correction, query snapshot, retention, top-N | PostgreSQL failover, distributed services |
| `npm run smoke:infra` | 4 API processes ended by `SIGKILL`; all exact retries recovered | response-loss recovery after four real commit boundaries | power loss, host/disk failure, external queue/sink |
| `npm run benchmark:infra` | fixed fixture completed | bounded observation for this implementation/runtime/database | production throughput, latency, cost, SLA |
| `npm audit --audit-level=high` | 0 vulnerabilities at lock generation | current registry advisory result | absence of unknown/supply-chain vulnerabilities |

## True-process crash receipt

The smoke uses separate operating-system processes and kills them after these repository methods have committed but before the HTTP
response is written:

1. ingest commit: exact batch retry recovers batch revision `1`;
2. event disposition + provisional count + checkpoint commit: exact process retry recovers checkpoint `1`;
3. publication commit: exact mutation retry recovers publication revision `1`;
4. correction commit: exact correction retry recovers publication revision `2`.

The pre-correction publication remains count `1`; the new corrected publication is count `2`. The receipt explicitly reports
fraud validity, attribution, billing, charge, and external acceptance as false.

## Fixed benchmark fixture and local observation

Fixture:

- 2 partitions, 34 batches, and 34 process transactions;
- 2,048 submitted events, 2,018 new canonical events, and 30 exact duplicate references;
- 16 minute windows, 32 ads, 2 regions, and 2 devices;
- 64 materialized queries, 1 post-publication late event, and 1 correction.

One local Node `v26.7.0` / PostgreSQL `17.11` run observed:

| Observation | Value |
|---|---:|
| ingest | 3,530.937 submitted events/s; 580.016 ms total |
| process transaction | p50 9.638 ms; p95 10.394 ms |
| full-snapshot publication | 7.573 ms |
| materialized query | p50 1.783 ms; p95 2.835 ms |
| one correction publication | 3.111 ms |

This was one sequential local run. It is not a comparison between Node versions and excludes network queueing, distributed
checkpointing, database replication/failover, real OLAP storage, fraud/attribution/billing, and production capacity/SLA.

## Failures found before public CI

The local gate caught four implementation/fixture issues before any public run existed:

1. the integration helper name `process` shadowed Node's global `process.env`;
2. the reset helper sent multiple parameterized SQL commands through one prepared statement;
3. the benchmark attempted to model a duplicate ID with a changed event time, correctly triggering whole-batch conflict;
4. the first corrected fixture referenced event IDs that had not been created in every prior batch, so its expected duplicate count
   was wrong.

They were fixed in the test/helper layer without weakening event conflict, transaction, or count semantics. The eventual public CI
history must disclose whether its first run is green; local fixes do not count as public evidence.

## Remaining completion gates

- repository policy/link/privacy scan and full local `check:ci`;
- implementation commit with clean worktree;
- public GitHub repository, topics, description, and default branch;
- public Node 22/24/26 + PostgreSQL 17.11 run with zero skips and exact per-job results;
- final documentation commit and a second green public run if evidence text changes;
- parent project card/plan/restore metadata update bound to the final remote commit.
