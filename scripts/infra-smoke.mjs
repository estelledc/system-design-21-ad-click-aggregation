import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { AggregationRepository, initializeDatabase, resetDatabase } from '../src/repository.js';

const { Pool } = pg;
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required; infrastructure smoke never skips');

const root = fileURLToPath(new URL('..', import.meta.url));
const apiToken = 'synthetic-smoke-token-0001';
const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 4, statement_timeout: 10_000 });
const repository = new AggregationRepository(pool);
const children = new Set();

function exitReceipt(child) {
  return new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
}

async function startServer(extraEnv = {}) {
  const child = spawn(process.execPath, ['src/main.js', 'serve'], {
    cwd: root,
    env: { ...process.env, AD_CLICK_API_TOKEN: apiToken, PORT: '0', ...extraEnv },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  children.add(child);
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  return new Promise((resolve, reject) => {
    const onExit = (code, signal) => reject(new Error(
      `server exited before readiness: code=${code} signal=${signal} stderr=${stderr}`,
    ));
    child.once('exit', onExit);
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
      const lines = stdout.split('\n');
      stdout = lines.pop();
      for (const line of lines) {
        if (!line) continue;
        const event = JSON.parse(line);
        if (event.event === 'ad_click_aggregation_ready') {
          child.off('exit', onExit);
          resolve({ child, base: `http://127.0.0.1:${event.port}` });
        }
      }
    });
  });
}

async function stop(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = exitReceipt(child);
  child.kill('SIGTERM');
  await exited;
}

async function post(base, path, body, { key, expected = 201 } = {}) {
  const response = await fetch(`${base}${path}`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiToken}`,
      'content-type': 'application/json',
      ...(key ? { 'idempotency-key': key } : {}),
    },
    body: JSON.stringify(body),
  });
  const value = await response.json();
  assert.equal(response.status, expected, JSON.stringify(value));
  return value;
}

async function state(base) {
  const response = await fetch(`${base}/v1/state`, { headers: { authorization: `Bearer ${apiToken}` } });
  assert.equal(response.status, 200);
  return response.json();
}

async function crashPost(server, path, body, key) {
  const exited = exitReceipt(server.child);
  await assert.rejects(fetch(`${server.base}${path}`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiToken}`,
      'content-type': 'application/json',
      ...(key ? { 'idempotency-key': key } : {}),
    },
    body: JSON.stringify(body),
  }));
  const receipt = await exited;
  assert.equal(receipt.signal, 'SIGKILL');
  return receipt;
}

const baseEvent = {
  eventId: 'event-smoke-base',
  eventTime: '2026-08-19T10:00:30.000Z',
  adId: 'ad-smoke',
  region: 'region-smoke',
  device: 'mobile',
};
const baseBatch = {
  sourceId: 'source-smoke-a',
  sourceEpoch: 0,
  batchSequence: 0,
  sourceWatermark: '2026-08-19T10:05:00.000Z',
  events: [baseEvent],
};

let api;
try {
  await initializeDatabase(pool);
  await resetDatabase(pool, 'ad-click-aggregation-lab-reset');

  const ingestCrashApi = await startServer({ AD_CLICK_CRASH_AFTER_INGEST_COMMIT: '1' });
  await post(ingestCrashApi.base, '/v1/sources', { sourceId: 'source-smoke-a', partition: 0 });
  await post(ingestCrashApi.base, '/v1/sources', { sourceId: 'source-smoke-b', partition: 1 });
  await post(ingestCrashApi.base, '/v1/assignments', {
    partition: 0,
    workerId: 'worker-smoke-a',
    expectedGeneration: 0,
  }, { key: 'smoke-assign-0000001' });
  await post(ingestCrashApi.base, '/v1/assignments', {
    partition: 1,
    workerId: 'worker-smoke-b',
    expectedGeneration: 0,
  }, { key: 'smoke-assign-0000002' });
  const ingestCrash = await crashPost(ingestCrashApi, '/v1/click-batches', baseBatch);

  api = await startServer();
  let replay = await post(api.base, '/v1/click-batches', baseBatch, { expected: 200 });
  assert.equal(replay.replayed, true);
  await post(api.base, '/v1/click-batches', {
    sourceId: 'source-smoke-b',
    sourceEpoch: 0,
    batchSequence: 0,
    sourceWatermark: '2026-08-19T10:05:00.000Z',
    events: [],
  });
  await post(api.base, '/v1/workers/worker-smoke-b/process', {
    partition: 1,
    generation: 1,
    expectedBatchRevision: 1,
  }, { key: 'smoke-process-b-0001' });

  await stop(api.child);
  const processCrashApi = await startServer({ AD_CLICK_CRASH_AFTER_PROCESS_COMMIT: '1' });
  const processCrash = await crashPost(processCrashApi, '/v1/workers/worker-smoke-a/process', {
    partition: 0,
    generation: 1,
    expectedBatchRevision: 1,
  }, 'smoke-process-a-0001');

  api = await startServer();
  replay = await post(api.base, '/v1/workers/worker-smoke-a/process', {
    partition: 0,
    generation: 1,
    expectedBatchRevision: 1,
  }, { key: 'smoke-process-a-0001', expected: 200 });
  assert.equal(replay.replayed, true);
  assert.equal(replay.countedCount, 1);

  await stop(api.child);
  const publicationCrashApi = await startServer({ AD_CLICK_CRASH_AFTER_PUBLICATION_COMMIT: '1' });
  const publicationCrash = await crashPost(publicationCrashApi, '/v1/publications', {
    expectedPublicationRevision: 0,
  }, 'smoke-publish-000001');

  api = await startServer();
  replay = await post(api.base, '/v1/publications', {
    expectedPublicationRevision: 0,
  }, { key: 'smoke-publish-000001', expected: 200 });
  assert.equal(replay.replayed, true);
  assert.equal(replay.publicationRevision, 1);

  await post(api.base, '/v1/click-batches', {
    ...baseBatch,
    batchSequence: 1,
    sourceWatermark: '2026-08-19T10:06:00.000Z',
    events: [{ ...baseEvent, eventId: 'event-smoke-late', eventTime: '2026-08-19T10:00:45.000Z' }],
  });
  await post(api.base, '/v1/click-batches', {
    sourceId: 'source-smoke-b',
    sourceEpoch: 0,
    batchSequence: 1,
    sourceWatermark: '2026-08-19T10:06:00.000Z',
    events: [],
  });
  const late = await post(api.base, '/v1/workers/worker-smoke-a/process', {
    partition: 0,
    generation: 1,
    expectedBatchRevision: 2,
  }, { key: 'smoke-process-a-0002' });
  await post(api.base, '/v1/workers/worker-smoke-b/process', {
    partition: 1,
    generation: 1,
    expectedBatchRevision: 2,
  }, { key: 'smoke-process-b-0002' });
  assert.deepEqual([late.countedCount, late.lateCount], [0, 1]);

  await stop(api.child);
  const correctionCrashApi = await startServer({ AD_CLICK_CRASH_AFTER_CORRECTION_COMMIT: '1' });
  const correctionCrash = await crashPost(correctionCrashApi, '/v1/corrections', {
    correctionId: 'correction-smoke',
    eventId: 'event-smoke-late',
    expectedPublicationRevision: 1,
  }, 'smoke-correct-000001');

  api = await startServer();
  replay = await post(api.base, '/v1/corrections', {
    correctionId: 'correction-smoke',
    eventId: 'event-smoke-late',
    expectedPublicationRevision: 1,
  }, { key: 'smoke-correct-000001', expected: 200 });
  assert.equal(replay.replayed, true);
  assert.equal(replay.publicationRevision, 2);
  assert.equal(replay.billingProved, false);

  const oldQuery = await post(api.base, '/v1/queries', {
    kind: 'count',
    publicationRevision: 1,
    start: '2026-08-19T10:00:00.000Z',
    end: '2026-08-19T10:01:00.000Z',
    adId: 'ad-smoke',
    region: null,
    device: null,
    limit: null,
    pageSize: 1,
  }, { key: 'smoke-query-old-0001' });
  const newQuery = await post(api.base, '/v1/queries', {
    kind: 'count',
    publicationRevision: 2,
    start: '2026-08-19T10:00:00.000Z',
    end: '2026-08-19T10:01:00.000Z',
    adId: 'ad-smoke',
    region: null,
    device: null,
    limit: null,
    pageSize: 1,
  }, { key: 'smoke-query-new-0001' });
  assert.equal(oldQuery.rows[0].count, 1);
  assert.equal(newQuery.rows[0].count, 2);
  assert.equal(newQuery.rows[0].status, 'corrected');
  const finalState = await state(api.base);
  assert.equal(finalState.latestPublicationRevision, 2);
  assert.equal(finalState.countedEventCount, 1);
  assert.equal(finalState.pendingLateCount, 0);
  assert.equal(finalState.correctionCount, 1);

  process.stdout.write(`${JSON.stringify({
    evidence: 'true_process_crash_recovery',
    killedProcesses: [ingestCrash.signal, processCrash.signal, publicationCrash.signal, correctionCrash.signal],
    ingestReplayBatchRevision: 1,
    processReplayCheckpoint: 1,
    publicationReplayRevision: 1,
    correctionReplayRevision: 2,
    oldPublicationCount: oldQuery.rows[0].count,
    correctedPublicationCount: newQuery.rows[0].count,
    fraudValidityProved: false,
    attributionProved: false,
    billingProved: false,
    chargeProved: false,
    externalAcceptanceProved: false,
  })}\n`);
} finally {
  await Promise.all([...children].map((child) => stop(child)));
  await pool.end();
}
