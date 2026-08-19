import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';
import pg from 'pg';
import {
  AggregationRepository,
  initializeDatabase,
  resetDatabase,
} from '../../src/repository.js';
import { AggregationService } from '../../src/service.js';

// Infrastructure tests never skip: DATABASE_URL absence is a failed gate, not an omitted conclusion.
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required for PostgreSQL integration tests');

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 12, statement_timeout: 10_000 });
let repository;
let service;

function event(id, time = '2026-08-19T10:00:30.000Z', overrides = {}) {
  return {
    eventId: id,
    eventTime: time,
    adId: 'ad-001',
    region: 'region-a',
    device: 'mobile',
    ...overrides,
  };
}

function batch(sourceId, batchSequence, sourceWatermark, events, overrides = {}) {
  return {
    sourceId,
    sourceEpoch: 0,
    batchSequence,
    sourceWatermark,
    events,
    ...overrides,
  };
}

async function sources() {
  await service.createSource({ sourceId: 'source-a', partition: 0 });
  await service.createSource({ sourceId: 'source-b', partition: 1 });
}

async function assignments() {
  const left = await service.assign({
    principal: 'lab-operator',
    requestKey: 'assign-partition-0000',
    body: { partition: 0, workerId: 'worker-a', expectedGeneration: 0 },
  });
  const right = await service.assign({
    principal: 'lab-operator',
    requestKey: 'assign-partition-0001',
    body: { partition: 1, workerId: 'worker-b', expectedGeneration: 0 },
  });
  return { left, right };
}

async function processBatch(partition, workerId, generation, expectedBatchRevision, key) {
  return service.processBatch({
    requestKey: key,
    workerId,
    body: { partition, generation, expectedBatchRevision },
  });
}

async function countQuery({ key, revision = null, start = '2026-08-19T10:00:00.000Z', end = '2026-08-19T10:01:00.000Z' }) {
  return service.createQuery({
    principal: 'lab-analyst',
    requestKey: key,
    body: {
      kind: 'count',
      publicationRevision: revision,
      start,
      end,
      adId: 'ad-001',
      region: null,
      device: null,
      limit: null,
      pageSize: 1,
    },
  });
}

before(async () => {
  await initializeDatabase(pool);
  repository = new AggregationRepository(pool);
  service = new AggregationService(repository);
});

beforeEach(async () => {
  await resetDatabase(pool, 'ad-click-aggregation-lab-reset');
});

after(async () => {
  await pool.end();
});

test('durable batch retry, duplicate receipt, and changed-event rollback preserve source progress', async () => {
  await service.createSource({ sourceId: 'source-a', partition: 0 });
  const first = await service.ingest(batch(
    'source-a',
    0,
    '2026-08-19T10:02:00.000Z',
    [event('event-0001')],
  ));
  const replay = await service.ingest(batch(
    'source-a',
    0,
    '2026-08-19T10:02:00.000Z',
    [event('event-0001')],
  ));
  assert.equal(replay.batchId, first.batchId);
  assert.equal(replay.replayed, true);

  const duplicate = await service.ingest(batch(
    'source-a',
    1,
    '2026-08-19T10:03:00.000Z',
    [event('event-0001'), event('event-0002', '2026-08-19T10:01:00.000Z')],
  ));
  assert.deepEqual([duplicate.newCount, duplicate.duplicateCount], [1, 1]);

  await assert.rejects(service.ingest(batch(
    'source-a',
    2,
    '2026-08-19T10:04:00.000Z',
    [event('event-0001', undefined, { adId: 'ad-changed' }), event('event-should-rollback')],
  )), { code: 'conflict' });
  const source = await pool.query(`SELECT next_sequence, next_batch_revision, next_input_offset FROM aca_sources`);
  assert.deepEqual(source.rows[0], { next_sequence: '2', next_batch_revision: '3', next_input_offset: '2' });
  assert.equal((await pool.query(`SELECT count(*) FROM aca_events WHERE event_id = 'event-should-rollback'`)).rows[0].count, '0');
});

test('concurrent cross-partition application resend creates one canonical event and two durable batch receipts', async () => {
  await sources();
  const payload = event('event-shared');
  const [left, right] = await Promise.all([
    service.ingest(batch('source-a', 0, '2026-08-19T10:03:00.000Z', [payload])),
    service.ingest(batch('source-b', 0, '2026-08-19T10:03:00.000Z', [payload])),
  ]);
  assert.deepEqual([left.newCount, right.newCount].sort(), [0, 1]);
  assert.equal((await pool.query(`SELECT count(*) FROM aca_events`)).rows[0].count, '1');
  assert.equal((await pool.query(`SELECT count(*) FROM aca_batches`)).rows[0].count, '2');
});

test('contribution and checkpoint commit once while takeover fences the stale worker', async () => {
  await sources();
  await assignments();
  await service.ingest(batch(
    'source-a',
    0,
    '2026-08-19T10:03:00.000Z',
    [event('event-0001')],
  ));
  await service.ingest(batch(
    'source-a',
    1,
    '2026-08-19T10:04:00.000Z',
    [event('event-0002', '2026-08-19T10:01:30.000Z')],
  ));
  const first = await processBatch(0, 'worker-a', 1, 1, 'process-partition-0001');
  const replay = await processBatch(0, 'worker-a', 1, 1, 'process-partition-0001');
  assert.equal(first.countedCount, 1);
  assert.equal(replay.replayed, true);
  const takeover = await service.assign({
    principal: 'lab-operator',
    requestKey: 'takeover-partition-0001',
    body: { partition: 0, workerId: 'worker-c', expectedGeneration: 1 },
  });
  assert.equal(takeover.generation, 2);
  await assert.rejects(
    processBatch(0, 'worker-a', 1, 2, 'stale-process-0000001'),
    { code: 'fenced' },
  );
  const second = await processBatch(0, 'worker-c', 2, 2, 'process-partition-0002');
  assert.equal(second.countedCount, 1);
  const facts = await pool.query(`
    SELECT
      (SELECT count(*) FROM aca_event_dispositions) AS dispositions,
      (SELECT sum(count) FROM aca_window_partials) AS partial_count,
      (SELECT checkpoint_batch_revision FROM aca_assignments WHERE partition_id = 0) AS checkpoint
  `);
  assert.deepEqual(facts.rows[0], { dispositions: '2', partial_count: '2', checkpoint: '2' });
});

test('publication waits for the minimum processed partition watermark', async () => {
  await sources();
  await assignments();
  await service.ingest(batch(
    'source-a',
    0,
    '2026-08-19T10:05:00.000Z',
    [event('event-0001')],
  ));
  await service.ingest(batch('source-b', 0, '2026-08-19T10:02:00.000Z', []));
  await processBatch(0, 'worker-a', 1, 1, 'process-minimum-left-01');
  await processBatch(1, 'worker-b', 1, 1, 'process-minimum-right1');
  const first = await service.publish({
    principal: 'lab-operator',
    requestKey: 'publish-minimum-000001',
    body: { expectedPublicationRevision: 0 },
  });
  assert.equal(first.globalWatermark, '2026-08-19T10:02:00.000Z');
  assert.equal(first.finalizedThrough, '2026-08-19T10:00:00.000Z');
  assert.equal(first.publicationRowCount, 0);

  await service.ingest(batch('source-b', 1, '2026-08-19T10:05:00.000Z', []));
  await processBatch(1, 'worker-b', 1, 2, 'process-minimum-right2');
  const second = await service.publish({
    principal: 'lab-operator',
    requestKey: 'publish-minimum-000002',
    body: { expectedPublicationRevision: 1 },
  });
  assert.equal(second.globalWatermark, '2026-08-19T10:05:00.000Z');
  assert.equal(second.finalizedThrough, '2026-08-19T10:03:00.000Z');
  assert.equal(second.publicationRowCount, 1);
});

test('late event creates a new correction publication while the old publication remains immutable', async () => {
  await sources();
  await assignments();
  await service.ingest(batch(
    'source-a',
    0,
    '2026-08-19T10:05:00.000Z',
    [event('event-ontime')],
  ));
  await service.ingest(batch('source-b', 0, '2026-08-19T10:05:00.000Z', []));
  await processBatch(0, 'worker-a', 1, 1, 'process-ontime-000001');
  await processBatch(1, 'worker-b', 1, 1, 'process-ontime-000002');
  const publication = await service.publish({
    principal: 'lab-operator',
    requestKey: 'publish-ontime-000001',
    body: { expectedPublicationRevision: 0 },
  });
  assert.equal(publication.publicationRevision, 1);

  await service.ingest(batch(
    'source-a',
    1,
    '2026-08-19T10:06:00.000Z',
    [event('event-late', '2026-08-19T10:00:45.000Z')],
  ));
  await service.ingest(batch('source-b', 1, '2026-08-19T10:06:00.000Z', []));
  const late = await processBatch(0, 'worker-a', 1, 2, 'process-late-0000001');
  await processBatch(1, 'worker-b', 1, 2, 'process-late-0000002');
  assert.deepEqual([late.countedCount, late.lateCount], [0, 1]);

  const before = await countQuery({ key: 'query-before-0000001', revision: 1 });
  assert.equal(before.rows[0].count, 1);
  assert.equal(before.rows[0].status, 'finalized');
  const correction = await service.correct({
    principal: 'lab-operator',
    requestKey: 'correct-late-0000001',
    body: {
      correctionId: 'correction-0001',
      eventId: 'event-late',
      expectedPublicationRevision: 1,
    },
  });
  assert.equal(correction.publicationRevision, 2);
  const oldAgain = await countQuery({ key: 'query-old-again-0001', revision: 1 });
  const corrected = await countQuery({ key: 'query-corrected-0001', revision: 2 });
  assert.equal(oldAgain.rows[0].count, 1);
  assert.equal(corrected.rows[0].count, 2);
  assert.equal(corrected.rows[0].status, 'corrected');
  assert.equal(corrected.billingProved, false);
  assert.equal((await pool.query(`SELECT count(*) FROM aca_publication_rows WHERE publication_revision = 1`)).rows[0].count, '1');
});

test('query receipt and page stay bound to their publication across a later correction', async () => {
  await sources();
  await assignments();
  await service.ingest(batch(
    'source-a', 0, '2026-08-19T10:05:00.000Z', [event('event-base')],
  ));
  await service.ingest(batch('source-b', 0, '2026-08-19T10:05:00.000Z', []));
  await processBatch(0, 'worker-a', 1, 1, 'query-process-base-01');
  await processBatch(1, 'worker-b', 1, 1, 'query-process-base-02');
  await service.publish({
    principal: 'lab-operator',
    requestKey: 'query-publish-base-01',
    body: { expectedPublicationRevision: 0 },
  });
  const frozen = await countQuery({ key: 'stable-query-00000001' });
  const pageBefore = await repository.getQueryPage(frozen.queryId, 0);

  await service.ingest(batch(
    'source-a', 1, '2026-08-19T10:06:00.000Z', [event('event-late')],
  ));
  await processBatch(0, 'worker-a', 1, 2, 'query-process-late-01');
  await service.correct({
    principal: 'lab-operator',
    requestKey: 'query-correct-late-01',
    body: { correctionId: 'correction-query', eventId: 'event-late', expectedPublicationRevision: 1 },
  });
  const pageAfter = await repository.getQueryPage(frozen.queryId, 0);
  assert.deepEqual(pageAfter, pageBefore);
  assert.equal(pageAfter.publicationRevision, 1);
  assert.equal(pageAfter.rows[0].count, 1);
  await assert.rejects(service.createQuery({
    principal: 'lab-analyst',
    requestKey: 'stable-query-00000001',
    body: {
      kind: 'count',
      publicationRevision: 2,
      start: '2026-08-19T10:00:00.000Z',
      end: '2026-08-19T10:01:00.000Z',
      adId: 'ad-001',
      region: null,
      device: null,
      limit: null,
      pageSize: 1,
    },
  }), { code: 'conflict' });
});

test('retention preserves publications, replays old batches, and rejects new events behind the dedup floor', async () => {
  await sources();
  await assignments();
  const original = batch(
    'source-a', 0, '2026-08-19T10:05:00.000Z', [event('event-retained')],
  );
  await service.ingest(original);
  await service.ingest(batch('source-b', 0, '2026-08-19T10:05:00.000Z', []));
  await processBatch(0, 'worker-a', 1, 1, 'retain-process-left-01');
  await processBatch(1, 'worker-b', 1, 1, 'retain-process-right01');
  await service.publish({
    principal: 'lab-operator',
    requestKey: 'retain-publish-000001',
    body: { expectedPublicationRevision: 0 },
  });
  const retained = await service.retain({
    principal: 'lab-operator',
    requestKey: 'retain-advance-0000001',
    body: { eventTimeFloor: '2026-08-19T10:01:00.000Z', expectedPublicationRevision: 1 },
  });
  assert.equal(retained.removedEventLedgerRows, 1);
  assert.equal((await pool.query(`SELECT count(*) FROM aca_events`)).rows[0].count, '0');
  assert.equal((await service.ingest(original)).replayed, true);
  await assert.rejects(service.ingest(batch(
    'source-a', 1, '2026-08-19T10:06:00.000Z', [event('event-too-old')],
  )), { code: 'beyond_dedup_horizon' });
  const source = await pool.query(`SELECT next_sequence FROM aca_sources WHERE source_id = 'source-a'`);
  assert.equal(source.rows[0].next_sequence, '1');
  const query = await countQuery({ key: 'retain-query-00000001', revision: 1 });
  assert.equal(query.rows[0].count, 1);
  assert.equal(query.rows[0].rawEvidence, 'purged');
});

test('top-N query is deterministic and rejects an unpublished range', async () => {
  await sources();
  await assignments();
  await service.ingest(batch('source-a', 0, '2026-08-19T10:05:00.000Z', [
    event('event-a', undefined, { adId: 'ad-a' }),
    event('event-b', undefined, { adId: 'ad-b' }),
  ]));
  await service.ingest(batch('source-b', 0, '2026-08-19T10:05:00.000Z', []));
  await processBatch(0, 'worker-a', 1, 1, 'top-process-left-0001');
  await processBatch(1, 'worker-b', 1, 1, 'top-process-right-001');
  await service.publish({
    principal: 'lab-operator',
    requestKey: 'top-publish-00000001',
    body: { expectedPublicationRevision: 0 },
  });
  const top = await service.createQuery({
    principal: 'lab-analyst',
    requestKey: 'top-query-0000000001',
    body: {
      kind: 'top',
      publicationRevision: 1,
      start: '2026-08-19T10:00:00.000Z',
      end: '2026-08-19T10:01:00.000Z',
      adId: null,
      region: null,
      device: null,
      limit: 2,
      pageSize: 2,
    },
  });
  assert.deepEqual(top.rows, [{ adId: 'ad-a', count: 1 }, { adId: 'ad-b', count: 1 }]);
  await assert.rejects(service.createQuery({
    principal: 'lab-analyst',
    requestKey: 'top-unpublished-00001',
    body: {
      kind: 'top',
      publicationRevision: 1,
      start: '2026-08-19T10:00:00.000Z',
      end: '2026-08-19T10:04:00.000Z',
      adId: null,
      region: null,
      device: null,
      limit: 2,
      pageSize: 2,
    },
  }), { code: 'window_not_published' });
});
