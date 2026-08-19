# Source comparison and evidence log

## Evidence method

The closed-book contract was committed as `0150c6157866331e47ff78b186590a0890b09f6d` before the fixed chapter was opened. This file
records what later reading confirmed, added, contradicted, or left unproved. It does not silently rewrite the baseline.

The secondary repository had no detected license file at the pinned revision. No prose, diagram, image, table, code, or benchmark
was copied. Only a clean-room summary of system-design ideas is retained here.

## Fixed secondary source

- Repository: [liquidslr/system-design-notes](https://github.com/liquidslr/system-design-notes).
- Repository revision: [`9d8388721e7231442763ad37398b8d82224aa68f`](https://github.com/liquidslr/system-design-notes/commit/9d8388721e7231442763ad37398b8d82224aa68f), committed 2026-08-12.
- Chapter: [Chapter 21: Ad Click Event Aggregation](https://github.com/liquidslr/system-design-notes/blob/9d8388721e7231442763ad37398b8d82224aa68f/21.%20Ad%20Click%20Event%20Aggregation/README.md).
- Chapter blob: `5b6cde66a014d10dedcd6ec311b5de740edbf96a`.

The chapter's one-billion-clicks/day prompt implies about 11,574 events/s, not exactly 10,000. Its 50,000 peak estimate is an
order-of-magnitude exercise, not a current platform fact. This lab keeps the one-billion/day premise only as a hypothetical failure
envelope and never treats its storage/QPS figures as measured requirements.

## Closed-book comparison

### Confirmed concerns

The chapter independently reinforces these closed-book concerns:

- event time differs from processing time and delayed events must have an explicit policy;
- duplicate delivery and response loss can corrupt counts;
- input position and aggregation state must recover together;
- raw history enables replay/recalculation and derived aggregates accelerate reads;
- near-real-time output and offline reconciliation are distinct paths;
- tumbling windows fit minute counts, while wider/top-N queries need aggregation across buckets;
- dimension pre-aggregation can explode state cardinality;
- hot ads, queue lag, worker failure, snapshots, replay, and backpressure are first-class design questions.

### Useful additions

The chapter added explicit dashboard queries for one ad and top-N ads, a raw-plus-derived storage split, map/local/reduce intuition,
dedicated replay isolation, resource/lag monitoring, and an end-of-day reconciliation path. Those ideas shape the bounded query,
benchmark, and operations plans. They do not select a production database for this lab.

### Material conflicts or overclaims

1. **Watermark is not an extended part of a window.** The chapter describes extending a window and calls that extension the
   watermark. Beam defines a watermark as an estimate of event-time completeness/progress; Flink separately defines a watermark,
   the window end, and allowed lateness. The v0.1 model therefore stores the watermark and allowed-lateness policy separately.
2. **Exactly once needs a named boundary.** A second queue does not by itself make source receipt, state mutation, checkpoint,
   database visibility, billing, and external effects atomic. Flink explicitly scopes its base claim to one event affecting managed
   state once; end-to-end also requires replayable sources and transactional or idempotent sinks. Kafka producer idempotence does
   not deduplicate application-level resends.
3. **Offsets are not stable click identity.** A transport offset can identify one log position, but the same logical event can be
   resent into another position or partition. The lab keeps a global synthetic `eventId` plus immutable payload digest in addition
   to source epoch, batch sequence, batch revision, and partition position.
4. **Final output cannot mutate invisibly.** Reconciliation or late data must create another visible publication/correction
   revision. The chapter does not define immutable query snapshots or how a caller distinguishes provisional, final, and corrected
   counts.
5. **Aggregate count is not a billing receipt.** A deduplicated click aggregate says nothing about genuine-human activity,
   attribution, contract eligibility, price, ledger entry, charge, settlement, invoice, or advertiser acceptance.

### Important omissions

- canonical event identity and changed-duplicate conflicts;
- half-open UTC window boundaries and deterministic tie-breaking;
- per-partition watermark merging, lagging inputs, and explicit idle-source policy;
- assignment generation and stale-worker fencing;
- atomic contribution/checkpoint semantics in the actual state store;
- finalization basis, publication manifest, correction generation, and query snapshot;
- dimension schema generation and mutable-enrichment hazards;
- deduplication retention versus replay horizon;
- privacy/consent boundaries around user and IP dimensions;
- explicit partial/missing/retained-away versus numeric zero;
- which receipt, if any, is independently accepted by fraud, attribution, billing, finance, or an advertiser.

## Primary-source calibration

Each page below was acquired as public data and its normalized bundle passed the local TraceFetch structural/hash verification.
Receipt timestamps are acquisition observations, not trusted publication clocks.

### Beam model

[Apache Beam's model basics](https://beam.apache.org/documentation/basics/) separates window assignment, watermark, trigger, and
allowed lateness. It describes a watermark as a source-produced estimate and says a window may expire only after its maximum
timestamp plus allowed lateness. This supports explicit provisional/final/corrected states rather than the chapter's single
"extended watermark" interval.

- Receipt: `tf_e668a046fccb5b93_20260818T235153Z`.
- Source SHA-256: `e668a046fccb5b93603bf312d4c9769f2e745f19a06674d9c358233daa599449`.

### Flink event time, windows, and recovery

The public rendered Flink nightlies pages were reachable but three direct normalization attempts failed. That failure is retained
as an acquisition limitation. The evidence used here instead comes from the official Apache Flink repository's `release-2.1`
branch at commit `903b3424ae313e91d8e3df29942191fdd7026c28`, fetched through fixed raw URLs and verified locally.

- [Generating Watermarks source](https://github.com/apache/flink/blob/903b3424ae313e91d8e3df29942191fdd7026c28/docs/content/docs/dev/datastream/event-time/generating_watermarks.md): parallel watermarks merge by the minimum; idle inputs otherwise hold progress back; watermark alignment limits fast-input drift. Receipt `tf_06ed190cee60bd2c_20260818T235316Z`, source SHA-256 `06ed190cee60bd2c3f5926e48ca3eb2f4f172c27badefb54d7e45f5eee7174ef`.
- [Timely Stream Processing source](https://github.com/apache/flink/blob/903b3424ae313e91d8e3df29942191fdd7026c28/docs/content/docs/concepts/time.md): an operator with multiple inputs uses the minimum event time; arbitrarily late events make absolute completeness impossible for heuristic watermarks. Receipt `tf_8b22e5d55fd512d3_20260818T235318Z`, source SHA-256 `8b22e5d55fd512d3419dfad0be0443f8341d58695686c6989914dbe855f1729f`.
- [Windows source](https://github.com/apache/flink/blob/903b3424ae313e91d8e3df29942191fdd7026c28/docs/content/docs/dev/datastream/operators/windows.md): time windows are start-inclusive/end-exclusive; allowed lateness is separate from the watermark and can produce another firing or a late side output. Receipt `tf_ee1f5c7b985775e0_20260818T235342Z`, source SHA-256 `ee1f5c7b985775e02ee6bfad25f4821d44f105b490cbc123bba6399bc46dda95`.
- [Fault Tolerance source](https://github.com/apache/flink/blob/903b3424ae313e91d8e3df29942191fdd7026c28/docs/content/docs/learn-flink/fault_tolerance.md): a checkpoint binds source positions to operator state; exactly-once state does not mean one physical execution; end-to-end additionally needs replayable sources and transactional/idempotent sinks. Receipt `tf_06615990acad4a80_20260818T235321Z`, source SHA-256 `06615990acad4a80862e3b728ef98d07b7904fcfbed7e3e60d55325b887a3a6a`.

These documents explain Flink semantics. The lab does not implement or claim Flink compatibility.

### Kafka producer scope

[KafkaProducer 4.2 API documentation](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
states that producer idempotence removes duplicates from producer retries, but application-level resends cannot be deduplicated by
that mechanism. Transactional production spans Kafka partitions/topics and requires committed-only consumers for its Kafka
end-to-end claim. This justifies stable application event IDs and prevents the chapter's second queue from being treated as a
general external exactly-once proof.

- Receipt: `tf_c2bf5d0aca4ea5fc_20260818T235158Z`.
- Source SHA-256: `c2bf5d0aca4ea5fcaac59ddf3389060662adbef43d911dfc48112140660637db`.

### PostgreSQL authority

[PostgreSQL 17 transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html) documents command/transaction
snapshots and serializable execution with retryable serialization failures. [PostgreSQL 17 constraints](https://www.postgresql.org/docs/17/ddl-constraints.html)
documents unique and primary-key enforcement. The lab uses explicit row locks plus unique constraints inside short transactions;
it does not claim that PostgreSQL is the chapter's scale solution.

- Transaction isolation receipt `tf_70cf332f46ff2d05_20260818T235201Z`, source SHA-256 `70cf332f46ff2d0537fe7706a7922fee9f1d8092fe98a8ea05947fe7cc1c769d`.
- Constraints receipt `tf_f5bad0ce59a21e53_20260818T235203Z`, source SHA-256 `f5bad0ce59a21e53cf608cf0ff330f3e413d41685bd65a4b470dd54194a8cac3`.

## Resulting v0.1 decision

The runnable slice uses one PostgreSQL 17 authority and exactly two logical source partitions. It proves stable event/batch retry,
changed-duplicate rollback, sequential source revisions, generation-fenced workers, atomic event disposition plus checkpoint,
minimum processed watermark, immutable publication revisions, explicit late-event correction, snapshot-bound queries, and a
logical dedup horizon. It deliberately does not prove distributed stream processing, a production OLAP store, billing accuracy,
real click validity, or external acceptance.
