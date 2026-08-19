import assert from 'node:assert/strict';
import test from 'node:test';
import {
  checkConservation,
  classifyDisposition,
  finalizedThroughFor,
  materializeCountRows,
  materializeTopRows,
  minimumWatermark,
  pageRows,
  windowFor,
} from '../../src/model.js';

test('windowing is UTC, minute-aligned, and end-exclusive', () => {
  assert.deepEqual(windowFor('2026-08-19T10:00:59.999Z'), {
    windowStart: '2026-08-19T10:00:00.000Z',
    windowEnd: '2026-08-19T10:01:00.000Z',
  });
  assert.deepEqual(windowFor('2026-08-19T10:01:00.000Z'), {
    windowStart: '2026-08-19T10:01:00.000Z',
    windowEnd: '2026-08-19T10:02:00.000Z',
  });
});

test('global watermark minimum and allowed lateness determine policy cutoff', () => {
  assert.equal(minimumWatermark([
    '2026-08-19T10:05:00.000Z',
    '2026-08-19T10:03:30.000Z',
  ]), '2026-08-19T10:03:30.000Z');
  assert.equal(finalizedThroughFor('2026-08-19T10:03:30.000Z'), '2026-08-19T10:01:00.000Z');
});

test('events behind an existing publication cutoff become late instead of mutating base state', () => {
  assert.equal(classifyDisposition('2026-08-19T10:01:00.000Z', null), 'counted');
  assert.equal(
    classifyDisposition('2026-08-19T10:01:00.000Z', '2026-08-19T10:01:00.000Z'),
    'late_unapplied',
  );
  assert.equal(
    classifyDisposition('2026-08-19T10:02:00.000Z', '2026-08-19T10:01:00.000Z'),
    'counted',
  );
});

test('count rows distinguish zero, corrected, unpublished, and raw evidence retention', () => {
  const rows = materializeCountRows({
    rows: [
      {
        windowStart: '2026-08-19T10:00:00.000Z',
        windowEnd: '2026-08-19T10:01:00.000Z',
        adId: 'ad-001',
        region: 'region-a',
        device: 'mobile',
        count: 2,
      },
    ],
    corrections: [{
      windowStart: '2026-08-19T10:00:00.000Z',
      adId: 'ad-001',
      region: 'region-a',
      device: 'mobile',
    }],
    query: {
      start: '2026-08-19T10:00:00.000Z',
      end: '2026-08-19T10:03:00.000Z',
      adId: 'ad-001',
      region: null,
      device: null,
    },
    finalizedThrough: '2026-08-19T10:02:00.000Z',
    retentionFloor: '2026-08-19T10:01:00.000Z',
  });
  assert.deepEqual(rows, [
    {
      windowStart: '2026-08-19T10:00:00.000Z',
      windowEnd: '2026-08-19T10:01:00.000Z',
      status: 'corrected',
      count: 2,
      rawEvidence: 'purged',
    },
    {
      windowStart: '2026-08-19T10:01:00.000Z',
      windowEnd: '2026-08-19T10:02:00.000Z',
      status: 'finalized',
      count: 0,
      rawEvidence: 'retained',
    },
    {
      windowStart: '2026-08-19T10:02:00.000Z',
      windowEnd: '2026-08-19T10:03:00.000Z',
      status: 'not_published',
      count: null,
      rawEvidence: 'not_applicable',
    },
  ]);
});

test('top rows aggregate windows and break count ties by ad ID', () => {
  const rows = materializeTopRows({
    rows: [
      { windowStart: '2026-08-19T10:00:00.000Z', adId: 'ad-b', region: 'r', device: 'mobile', count: 2 },
      { windowStart: '2026-08-19T10:01:00.000Z', adId: 'ad-a', region: 'r', device: 'mobile', count: 2 },
      { windowStart: '2026-08-19T10:01:00.000Z', adId: 'ad-c', region: 'x', device: 'mobile', count: 9 },
    ],
    query: {
      start: '2026-08-19T10:00:00.000Z',
      end: '2026-08-19T10:02:00.000Z',
      region: 'r',
      device: null,
      limit: 2,
    },
  });
  assert.deepEqual(rows, [{ adId: 'ad-a', count: 2 }, { adId: 'ad-b', count: 2 }]);
});

test('pagination is bounded and conservation separates base from corrections', () => {
  assert.deepEqual(pageRows([1, 2, 3], 2, 1), { page: 1, pageCount: 2, rows: [3] });
  assert.equal(pageRows([1], 1, 1), null);
  assert.deepEqual(checkConservation({
    dispositions: ['counted', 'late_unapplied', 'counted'],
    partials: [1, 1],
    corrections: [1],
  }), {
    valid: true,
    counted: 2,
    late: 1,
    base: 2,
    correctionTotal: 1,
    publishedTotal: 3,
  });
});

test('generated timestamps preserve half-open assignment and monotonic publication cutoffs', () => {
  let state = 0x21ad2026;
  const random = () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
  const origin = Date.parse('2026-08-19T00:00:00.000Z');
  let priorWatermark = '2026-08-19T00:02:00.000Z';
  let priorCutoff = finalizedThroughFor(priorWatermark);
  for (let index = 0; index < 2_000; index += 1) {
    const instant = origin + (random() % (24 * 60 * 60 * 1000));
    const assigned = windowFor(new Date(instant).toISOString());
    assert.ok(new Date(assigned.windowStart).getTime() <= instant);
    assert.ok(instant < new Date(assigned.windowEnd).getTime());
    assert.equal(new Date(assigned.windowEnd).getTime() - new Date(assigned.windowStart).getTime(), 60_000);

    const nextWatermark = new Date(new Date(priorWatermark).getTime() + (random() % 90_000)).toISOString();
    const nextCutoff = finalizedThroughFor(nextWatermark);
    assert.ok(nextWatermark >= priorWatermark);
    assert.ok(nextCutoff >= priorCutoff);
    priorWatermark = nextWatermark;
    priorCutoff = nextCutoff;
  }
});
