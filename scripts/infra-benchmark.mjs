import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import pg from 'pg';
import {
  normalizeAssignment,
  normalizeBatch,
  normalizeCorrection,
  normalizeProcess,
  normalizePublication,
  normalizeQuery,
} from '../src/contracts.js';
import { AggregationRepository, initializeDatabase, resetDatabase } from '../src/repository.js';

const { Pool } = pg;
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required; infrastructure benchmark never skips');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 12, statement_timeout: 20_000 });
const repository = new AggregationRepository(pool);

function percentile(values, p) {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
}

function round(value) {
  return Math.round(value * 1000) / 1000;
}

function event(partition, batch, index, duplicate = false) {
  const sequence = duplicate ? 0 : batch;
  const eventIndex = duplicate ? 0 : index;
  const minute = (sequence * 3 + eventIndex) % 16;
  return {
    eventId: `event-p${partition}-b${sequence.toString().padStart(2, '0')}-i${eventIndex.toString().padStart(2, '0')}`,
    eventTime: `2026-08-19T10:${minute.toString().padStart(2, '0')}:${(eventIndex % 60).toString().padStart(2, '0')}.000Z`,
    adId: `ad-${(eventIndex % 32).toString().padStart(2, '0')}`,
    region: `region-${eventIndex % 2}`,
    device: eventIndex % 2 ? 'mobile' : 'desktop',
  };
}

try {
  await initializeDatabase(pool);
  await resetDatabase(pool, 'ad-click-aggregation-lab-reset');
  await repository.createSource({ sourceId: 'benchmark-source-a', partition: 0 });
  await repository.createSource({ sourceId: 'benchmark-source-b', partition: 1 });
  for (const request of [
    { partition: 0, workerId: 'benchmark-worker-a', expectedGeneration: 0 },
    { partition: 1, workerId: 'benchmark-worker-b', expectedGeneration: 0 },
  ]) {
    const normalized = normalizeAssignment(request);
    await repository.assign({
      principal: 'benchmark-operator',
      requestKey: `benchmark-assign-${request.partition.toString().padStart(4, '0')}`,
      ...normalized,
    });
  }

  const submittedEvents = 2 * 16 * 64;
  const expectedDuplicates = 2 * 15;
  const ingestStarted = performance.now();
  let newEvents = 0;
  let duplicates = 0;
  for (let partition = 0; partition < 2; partition += 1) {
    for (let batchSequence = 0; batchSequence < 16; batchSequence += 1) {
      const events = Array.from({ length: 64 }, (_, index) => (
        batchSequence > 0 && index === 0
          ? event(partition, batchSequence, index, true)
          : event(partition, batchSequence, index)
      ));
      const result = await repository.ingest(normalizeBatch({
        sourceId: partition === 0 ? 'benchmark-source-a' : 'benchmark-source-b',
        sourceEpoch: 0,
        batchSequence,
        sourceWatermark: '2026-08-19T10:20:00.000Z',
        events,
      }));
      newEvents += result.newCount;
      duplicates += result.duplicateCount;
    }
  }
  const ingestMs = performance.now() - ingestStarted;
  assert.equal(newEvents, submittedEvents - expectedDuplicates);
  assert.equal(duplicates, expectedDuplicates);

  const processLatencies = [];
  for (let partition = 0; partition < 2; partition += 1) {
    for (let batchRevision = 1; batchRevision <= 16; batchRevision += 1) {
      const started = performance.now();
      const request = normalizeProcess({ partition, generation: 1, expectedBatchRevision: batchRevision });
      await repository.processBatch({
        requestKey: `benchmark-process-${partition}-${batchRevision.toString().padStart(4, '0')}`,
        workerId: partition === 0 ? 'benchmark-worker-a' : 'benchmark-worker-b',
        ...request,
      });
      processLatencies.push(performance.now() - started);
    }
  }
  const publicationRequest = normalizePublication({ expectedPublicationRevision: 0 });
  const publicationStarted = performance.now();
  const publication = await repository.publish({
    principal: 'benchmark-operator',
    requestKey: 'benchmark-publish-0001',
    ...publicationRequest,
  });
  const publicationMs = performance.now() - publicationStarted;

  const queryLatencies = [];
  for (let index = 0; index < 64; index += 1) {
    const request = normalizeQuery({
      kind: index % 4 === 0 ? 'top' : 'count',
      publicationRevision: publication.publicationRevision,
      start: '2026-08-19T10:00:00.000Z',
      end: '2026-08-19T10:16:00.000Z',
      adId: index % 4 === 0 ? null : `ad-${(index % 32).toString().padStart(2, '0')}`,
      region: null,
      device: null,
      limit: index % 4 === 0 ? 10 : null,
      pageSize: 10,
    });
    const started = performance.now();
    await repository.createQuery({
      principal: 'benchmark-analyst',
      requestKey: `benchmark-query-${index.toString().padStart(4, '0')}`,
      ...request,
    });
    queryLatencies.push(performance.now() - started);
  }

  const lateBatch = await repository.ingest(normalizeBatch({
    sourceId: 'benchmark-source-a',
    sourceEpoch: 0,
    batchSequence: 16,
    sourceWatermark: '2026-08-19T10:21:00.000Z',
    events: [{
      eventId: 'event-benchmark-late',
      eventTime: '2026-08-19T10:00:30.000Z',
      adId: 'ad-00',
      region: 'region-0',
      device: 'desktop',
    }],
  }));
  await repository.ingest(normalizeBatch({
    sourceId: 'benchmark-source-b',
    sourceEpoch: 0,
    batchSequence: 16,
    sourceWatermark: '2026-08-19T10:21:00.000Z',
    events: [],
  }));
  for (const partition of [0, 1]) {
    const request = normalizeProcess({ partition, generation: 1, expectedBatchRevision: 17 });
    await repository.processBatch({
      requestKey: `benchmark-late-process-${partition}`,
      workerId: partition === 0 ? 'benchmark-worker-a' : 'benchmark-worker-b',
      ...request,
    });
  }
  assert.equal(lateBatch.newCount, 1);
  const correctionRequest = normalizeCorrection({
    correctionId: 'benchmark-correction',
    eventId: 'event-benchmark-late',
    expectedPublicationRevision: publication.publicationRevision,
  });
  const correctionStarted = performance.now();
  await repository.correct({
    principal: 'benchmark-operator',
    requestKey: 'benchmark-correction-0001',
    ...correctionRequest,
  });
  const correctionMs = performance.now() - correctionStarted;

  process.stdout.write(`${JSON.stringify({
    evidence: 'bounded_synthetic_benchmark',
    runtime: process.version,
    postgresql: (await pool.query(`SHOW server_version`)).rows[0].server_version,
    fixture: {
      partitions: 2,
      batches: 34,
      submittedEvents,
      canonicalNewEvents: newEvents,
      duplicateReferences: duplicates,
      minuteWindows: 16,
      ads: 32,
      regions: 2,
      devices: 2,
      processTransactions: 34,
      queries: 64,
      lateEvents: 1,
      corrections: 1,
    },
    observations: {
      ingestEventsPerSecond: round(submittedEvents / (ingestMs / 1000)),
      ingestTotalMs: round(ingestMs),
      processP50Ms: round(percentile(processLatencies, 0.5)),
      processP95Ms: round(percentile(processLatencies, 0.95)),
      publicationMs: round(publicationMs),
      queryP50Ms: round(percentile(queryLatencies, 0.5)),
      queryP95Ms: round(percentile(queryLatencies, 0.95)),
      correctionMs: round(correctionMs),
    },
    exclusions: [
      'network queue',
      'distributed checkpoint',
      'database replication/failover',
      'real OLAP engine',
      'fraud/attribution/billing',
      'production capacity and SLA',
    ],
  })}\n`);
} finally {
  await pool.end();
}
