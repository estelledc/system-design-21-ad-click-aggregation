# Operations

## Runtime and startup

Use Node.js 22 or newer and PostgreSQL 17. The application requires:

- `DATABASE_URL`: disposable PostgreSQL database used as the only v0.1 authority;
- `AD_CLICK_API_TOKEN`: at least 16 characters, used only for loopback lab requests;
- `PORT`: optional loopback port, default `3000`; `0` selects an ephemeral port in smoke tests.

The server applies idempotent schema creation before listening. It never creates the database itself. Use a dedicated database;
integration, smoke, and benchmark gates intentionally reset all `aca_*` state.

## Standard gates

```sh
npm ci --ignore-scripts
npm run check
DATABASE_URL='postgres://...' npm run test:infra
DATABASE_URL='postgres://...' npm run smoke:infra
DATABASE_URL='postgres://...' npm run benchmark:infra
```

`npm run check:ci` runs all of them in order. Infrastructure scripts fail closed if `DATABASE_URL` is missing. No test has a skip
branch for an unavailable database.

## Healthy-state interpretation

`GET /health` proves only that the HTTP process is listening. `GET /v1/state` is the operational view:

- compare each partition's `checkpointBatchRevision` with `latestDurableBatchRevision` for process lag;
- compare `processedWatermark` with `sourceWatermark` for watermark lag;
- the lower processed watermark governs publication; one quiet partition does not disappear automatically;
- `pendingLateCount` is correction work, not another published count;
- `latestPublicationRevision`, `globalWatermark`, and `finalizedThrough` describe the current query snapshot policy;
- `retainedEventCount` and `retentionEventTimeFloor` describe the dedup/event-ledger boundary.

State output intentionally omits source, worker, event, ad, region, device, request, query, correction, digest, token, and payload
values. Production monitoring would need an independent path; this same process cannot prove its own availability or completeness.

## Recovery playbooks

### Producer lost an ingest response

Retry the exact source epoch, batch sequence, watermark, ordered events, and payload. A stable match returns the original batch ID,
batch revision, offsets, and counts. A changed retry is a conflict. Do not increment the source sequence until the prior receipt is
known.

### Worker died while processing

Retry the same worker generation, expected batch revision, and request key. A committed transaction returns its receipt. An
uncommitted transaction left neither contribution nor checkpoint. If takeover is required, assign a new generation under the
expected current generation; the old worker is fenced from new mutations.

### Publication response was lost

Retry the same idempotency key and expected prior revision. A committed publication returns the original revision. Do not issue a
new key until the result is known, or the expected-revision fence will correctly reject ambiguity.

### Event appears behind the published cutoff

Processing stores it as pending late data and leaves the old publication unchanged. Inspect the event through an authorized
operator path outside ordinary logs, decide whether the synthetic event should be included, then submit one stable correction.
Correction is `+1` only and is not a fraud/billing action.

### Correction response was lost

Retry the same correction ID, request key, event ID, and expected prior publication. The committed correction returns the original
new publication. Reusing the correction or event with another intent conflicts.

### One watermark stalls

Check durable-versus-processed batch revision first. If the source is genuinely quiet, it must publish an explicit zero-event batch
with a monotonic source watermark; v0.1 has no timeout-based idleness rule. Advancing a watermark without source evidence would trade
an observable stall for silent incompleteness.

## Retention procedure

Retention is allowed only when:

1. the request names the latest publication;
2. both partition checkpoints equal their latest durable batch revisions;
3. the new minute-aligned floor is not beyond the published cutoff;
4. no uncorrected late event lies before the floor.

The transaction first establishes the rejection floor, then removes matching event-ledger rows and batch-event mappings while
preserving batch receipts, aggregate partials, correction lineage, and publication snapshots. Afterward, a new event older than the
floor returns `beyond_dedup_horizon`; it is never accepted as newly unique. This is a local logical retention model, not an archive,
legal hold, erasure workflow, or backup strategy.

## Benchmark interpretation

The benchmark prints exact fixture counts, Node/PostgreSQL versions, ingest throughput, process/query percentiles, publication and
correction time, and explicit exclusions. It is one sequential client against one local database. Do not compare runtime versions
without repeated controlled trials and do not extrapolate to the chapter's one-billion/day prompt.

## Unsupported production work

Before any production-like use, add independently reviewed privacy/consent rules, authentication/authorization, audit access,
dynamic partitions and handoff, durable raw archive, replay isolation, real stream/OLAP systems, schema evolution, negative/audited
adjustments, database HA/backup/restore, capacity tests, independent monitoring, incident response, and downstream fraud,
attribution, billing, and finance contracts. None is implied by this lab.
