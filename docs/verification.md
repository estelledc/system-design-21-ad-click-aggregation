# Verification ledger

## Current status

The identity-safe rewrite preserved every existing tree, message, and timestamp while mapping the four commits in order:

- `0150c6157866331e47ff78b186590a0890b09f6d` → `8a2f359e98b03e833fd0847234ee42b5ad7bbfdd`
- `046014a115834330309a9e1f55692dfa5b4f2fd4` → `a82c7a2d0edcd56656361050e12e841958dbce5b`
- `29385668c80475623bca7b34e5ff11ba981ba987` → `fc3458f8b9d3d69872a8633b83a66c8d8080aee0`
- `8f64c01093b6712e86d2700c16d72f3ecad1386c` → `def03aacf6828bde1d3db0f63f1e83ad6384b2a0`

Current reachable `main` uses the repository owner's GitHub noreply identity. Rewritten baseline
`def03aacf6828bde1d3db0f63f1e83ad6384b2a0` passed [CI run 32228055638](https://github.com/estelledc/system-design-21-ad-click-aggregation/actions/runs/32228055638)
on Node 22/24/26 with the pinned PostgreSQL 17.11 container and the complete gate.

Local implementation gates pass on Node `v26.7.0` and PostgreSQL `17.11 (Homebrew)`. Historical pre-rewrite
[GitHub Actions 32201204205](https://github.com/estelledc/system-design-21-ad-click-aggregation/actions/runs/32201204205) passed from
old implementation object `29385668c80475623bca7b34e5ff11ba981ba987`; its tree-equivalent current commit is
`fc3458f8b9d3d69872a8633b83a66c8d8080aee0`, and the run remains bound to the old object. External advertising, billing,
capacity, SLA, and product acceptance remain unproved and out of scope.

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
history shows that the first implementation run was green; the fixes still count only as synthetic repository evidence.

## First public implementation run

Run `32201204205` used PostgreSQL `17.11` in every job. Each job passed the repository policy/link/privacy scan, dependency audit,
18 pure tests, 8 real-PostgreSQL tests, four-process crash smoke, and the fixed benchmark with zero test failures, skips, or todos.

| Job | Runtime | Result | Ingest | Process p50 / p95 | Publish | Query p50 / p95 | Correction |
|---|---|---|---:|---:|---:|---:|---:|
| [`95915230356`](https://github.com/estelledc/system-design-21-ad-click-aggregation/actions/runs/32201204205/job/95915230356) | Node `v22.23.2` | 18 + 8 tests; 0 fail/skip/todo | 955.277 events/s | 41.533 / 47.313 ms | 25.015 ms | 6.445 / 7.935 ms | 20.833 ms |
| [`95915230220`](https://github.com/estelledc/system-design-21-ad-click-aggregation/actions/runs/32201204205/job/95915230220) | Node `v24.19.0` | 18 + 8 tests; 0 fail/skip/todo | 993.397 events/s | 44.521 / 49.897 ms | 25.313 ms | 6.632 / 8.123 ms | 21.269 ms |
| [`95915230282`](https://github.com/estelledc/system-design-21-ad-click-aggregation/actions/runs/32201204205/job/95915230282) | Node `v26.7.0` | 18 + 8 tests; 0 fail/skip/todo | 1,045.033 events/s | 42.446 / 45.011 ms | 33.767 ms | 6.515 / 8.214 ms | 21.054 ms |

These are concurrent hosted-runner observations, not a controlled Node-version comparison. Runner load, shared database-container
resources, and scheduling differ; the values establish only that the bounded fixture completed and emitted its receipt.

## Completion accounting

- Repository policy/link/privacy scan and full local `check:ci`: complete.
- Implementation commit with clean worktree: complete at current equivalent `fc3458f8b9d3d69872a8633b83a66c8d8080aee0`.
- Public repository, topics, description, and default `main`: complete.
- First public Node 22/24/26 + PostgreSQL 17.11 run: complete and green at `32201204205`.
- Documentation evidence update: this commit must pass the same public matrix before the final remote commit is recorded in the
  parent restore metadata.
- External acceptance: not requested, not run, and not implied by repository completion.
