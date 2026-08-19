# Threat model

## Protected assets

- canonical synthetic event identity and immutable payload meaning;
- source epoch/sequence, batch revision, input position, and original receipt;
- worker assignment generation, contribution, checkpoint, and processed watermark;
- provisional counts, late dispositions, correction lineage, publication revisions, and query receipts;
- bearer token and database credentials;
- the evidence boundary separating aggregate count from fraud, attribution, billing, money, and external acceptance.

All fixtures are synthetic. Real clickstream, advertiser, campaign, user, device, IP, cookie, mobile identifier, tracking URL,
pricing, invoice, fraud signal, and production topology are forbidden even though some are common in advertising systems.

## Trust boundaries

1. A synthetic producer supplies event identity, event time, dimensions, and a heuristic source watermark.
2. The loopback HTTP process validates/authenticates requests but is not an independent durable authority.
3. PostgreSQL is the sole v0.1 authority and failure domain.
4. An operator assigns workers, publishes cutoffs, applies corrections, and advances retention.
5. An analyst receives a materialized publication-bound query.
6. Fraud, attribution, billing, finance, report delivery, and advertiser systems are outside the repository.

## Threats and controls

| Threat | Control | Residual risk |
|---|---|---|
| response-loss resend increments count twice | stable batch identity, event ID/digest, original receipt, unique disposition | producer can mint another event ID for the same real action; no real identity proof exists |
| event ID reused with changed time/dimensions | whole-batch conflict rollback | no external correction/identity authority validates producer intent |
| application resend lands in another partition | event ID is global across both partitions | fixed two-partition lab does not prove distributed/global uniqueness at scale |
| stale source continues after restart | expected epoch and strict sequence | source credential ownership and distributed fencing are not implemented |
| stale worker writes after takeover | explicit generation checked inside the processing transaction | no lease/liveness detector; operator must initiate takeover |
| checkpoint skips an uncounted event | disposition/partial/receipt/checkpoint/watermark commit together | one PostgreSQL failure domain hides queue/state-store split-brain |
| one fast source causes false global finalization | required partition minimum; no implicit idle timeout | source watermark itself is heuristic and may be dishonest or buggy |
| late event rewrites a report invisibly | late ledger plus explicit correction/publication revision | v0.1 supports inclusion `+1` only, not invalidation/retraction |
| query mixes revisions while paging | complete result is materialized under one publication | stored JSON is bounded, not suitable for large analytics results |
| dimension explosion exhausts storage | three bounded dimensions, 64-event batch, bounded queries | active values are not globally capped in v0.1; database growth remains possible |
| replay after dedup state removal becomes a new click | event-time rejection floor precedes purge | bad event timestamps can cause legitimate old arrivals to be rejected |
| sensitive/high-cardinality values leak to logs | fixed low-cardinality structured log fields and repository scan | database rows and authorized API responses still contain synthetic identifiers |
| aggregate is consumed as a bill/charge | every query carries negative evidence flags; docs/API avoid money fields | an external caller can ignore the contract; no downstream policy enforcement exists |
| bearer/database secret leaks | environment-only credentials, no secret logging, repository scan | no vault, rotation, TLS, per-role auth, or deployed network boundary |
| self-monitoring hides a stalled process | docs state that `/health` and `/v1/state` are not independent proof | no independent monitor exists in v0.1 |

## Timestamp and watermark abuse

Canonical UTC timestamps are bounded to the lab envelope, but the source remains authoritative for event time. The source watermark
is monotonic and bound to a durable batch prefix; it is still a heuristic claim. The API and query receipts therefore say
`completenessProved: false`. A post-cutoff event is evidence that the prior heuristic was incomplete, so it enters correction flow
instead of mutating the old snapshot.

## Availability and denial of service

Request bodies, batches, identifiers, devices, query windows, pages, and top-N are bounded. PostgreSQL statement timeout and a small
pool limit individual work. Missing controls include tenant quotas, per-dimension cardinality caps, rate limits, queue backpressure,
disk budgets, compaction budgets, connection admission, and distributed hotspot mitigation. The lab must not be internet-exposed.

## Evidence and privacy boundary

Ordinary logs may include operation name, partition number, result counts, status/error code, and duration. They must not include
source/worker/event/ad/correction/query/request identifiers, dimensions, timestamps from payloads, filters, digests, tokens, bodies,
or database URLs. Repository checks scan portable files and explicit log sites, but they are not a substitute for runtime data-loss
prevention or human review.

## Residual conclusion

The lab can demonstrate a bounded database-state invariant under synthetic retries, concurrency, and process crashes. It cannot
establish source truth, human click validity, consent, attribution, fraud outcome, billing eligibility, price, financial ledger
correctness, charge, settlement, advertiser report delivery, production availability, or external acceptance.
