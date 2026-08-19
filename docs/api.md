# API and receipt contract

## Transport rules

- HTTP/1.1 JSON on loopback for the runnable lab.
- `Authorization: Bearer <AD_CLICK_API_TOKEN>` is required except for `GET /health`.
- Operator/query mutations require `Idempotency-Key`, 8–128 bounded identifier characters.
- Bodies reject unknown/missing keys, oversized input, non-canonical timestamps, unbounded identifiers, and forbidden dimensions.
- Ordinary logs contain only operation, status/error code, duration, and low-cardinality counts; no event, ad, source, worker,
  request-key, payload, digest, filter, or token value.

## Source routes

### `POST /v1/sources`

```json
{"sourceId":"source-a","partition":0}
```

Creates or exactly replays a source binding. One source may own each of partitions `0` and `1`.

### `POST /v1/sources/{sourceId}/epochs`

```json
{"expectedEpoch":0}
```

Rotates the epoch and resets only batch sequence. Requires `Idempotency-Key`; monotonic batch revision, input position, and latest
declared watermark do not reset.

### `POST /v1/click-batches`

```json
{
  "sourceId":"source-a",
  "sourceEpoch":0,
  "batchSequence":0,
  "sourceWatermark":"2026-08-19T10:03:00.000Z",
  "events":[
    {
      "eventId":"event-0001",
      "eventTime":"2026-08-19T10:00:59.999Z",
      "adId":"ad-001",
      "region":"region-a",
      "device":"mobile"
    }
  ]
}
```

The durable response names batch revision, input range, submitted/new/duplicate counts, stored watermark, and `aggregateVisible:
false`. Empty `events` is allowed only as an explicit partition-watermark advance.

## Worker and publication routes

### `POST /v1/assignments`

```json
{"partition":0,"workerId":"worker-a","expectedGeneration":0}
```

Creates the next assignment generation under `Idempotency-Key` without resetting checkpoint or processed watermark.

### `POST /v1/workers/{workerId}/process`

```json
{"partition":0,"generation":1,"expectedBatchRevision":1}
```

Processes exactly one next batch under `Idempotency-Key`. The receipt reports owned, duplicate-reference, counted, and late counts,
then the committed checkpoint and processed watermark. It never returns event or dimension values.

### `POST /v1/publications`

```json
{"expectedPublicationRevision":0}
```

Under `Idempotency-Key`, computes the minimum processed watermark, applies the fixed two-minute lateness policy, and creates the next
immutable full-snapshot publication only when its cutoff advances. Revision zero means no prior publication.

### `POST /v1/corrections`

```json
{
  "correctionId":"correction-0001",
  "eventId":"event-late-0001",
  "expectedPublicationRevision":1
}
```

Applies one `+1` correction to one stored `late_unapplied` event and creates a new publication. The response states
`aggregateOnly: true`, `billingProved: false`, and `chargeProved: false`.

### `POST /v1/retention`

```json
{"eventTimeFloor":"2026-08-19T10:01:00.000Z","expectedPublicationRevision":2}
```

Moves the logical replay/dedup floor and purges older event identity only when all source batches are processed, the floor is not
beyond the published cutoff, and no unapplied late event lies before it. Publication snapshots and batch receipts remain.

## Query routes

### `POST /v1/queries`

Count request:

```json
{
  "kind":"count",
  "publicationRevision":2,
  "start":"2026-08-19T10:00:00.000Z",
  "end":"2026-08-19T10:03:00.000Z",
  "adId":"ad-001",
  "region":null,
  "device":null,
  "limit":null,
  "pageSize":2
}
```

Top-N request uses `"kind":"top"`, `"adId":null`, and `"limit":10`. All other keys remain present so canonical intent is
unambiguous. `publicationRevision:null` freezes the latest revision at query creation.

Every response includes:

```json
{
  "measure":"deduplicated_synthetic_click_count",
  "watermarkKind":"source_declared_heuristic",
  "finalizedByPolicy":true,
  "completenessProved":false,
  "fraudValidityProved":false,
  "attributionProved":false,
  "billingProved":false,
  "chargeProved":false,
  "externalAcceptanceProved":false
}
```

### `GET /v1/queries/{queryId}/pages/{page}`

Returns a zero-based page from the stored query receipt, plus `publicationRevision`, `page`, `pageCount`, and `replayed: true`.

## State route

`GET /v1/state` exposes partition checkpoint/watermark lag, source/batch/event counts, counted/late/correction counts, latest
publication/cutoff, and retention floor. It omits all high-cardinality identities and values.

## Error vocabulary

| Code | Meaning |
|---|---|
| `invalid_request` | shape, type, timestamp, limit, boundary, or policy validation failed |
| `unauthorized` | missing/invalid bearer token |
| `not_found` | named source, event disposition, publication, or query does not exist |
| `conflict` | stable identity was reused with changed intent |
| `out_of_order` | source sequence or worker batch revision skipped/repeated unexpectedly |
| `fenced` | source epoch, assignment generation, or expected publication is stale |
| `no_work` | requested next source batch does not exist |
| `not_publishable` | global watermark does not advance the published cutoff |
| `window_not_published` | top-N range crosses the selected publication cutoff |
| `late_event_required` | correction target was not stored as unapplied late data |
| `beyond_dedup_horizon` | new event time precedes the logical event-identity floor |
| `retention_blocked` | unprocessed input, unpublished range, or unapplied late data prevents purge |
| `internal_error` | redacted unexpected failure; details stay server-side |
