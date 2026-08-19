import assert from 'node:assert/strict';
import test from 'node:test';
import {
  canonicalStringify,
  digestValue,
  normalizeBatch,
  normalizeQuery,
  normalizeRetention,
} from '../../src/contracts.js';

function event(overrides = {}) {
  return {
    eventId: 'event-0001',
    eventTime: '2026-08-19T10:00:59.999Z',
    adId: 'ad-001',
    region: 'region-a',
    device: 'mobile',
    ...overrides,
  };
}

function batch(events = [event()], overrides = {}) {
  return {
    sourceId: 'source-a',
    sourceEpoch: 0,
    batchSequence: 0,
    sourceWatermark: '2026-08-19T10:03:00.000Z',
    events,
    ...overrides,
  };
}

test('canonical JSON and digest do not depend on object key insertion order', () => {
  assert.equal(canonicalStringify({ b: 2, a: { d: 4, c: 3 } }), '{"a":{"c":3,"d":4},"b":2}');
  assert.equal(digestValue({ a: 1, b: 2 }), digestValue({ b: 2, a: 1 }));
});

test('batch freezes event identity and canonical half-open minute window', () => {
  const result = normalizeBatch(batch());
  assert.equal(result.events[0].windowStart, '2026-08-19T10:00:00.000Z');
  assert.equal(result.events[0].windowEnd, '2026-08-19T10:01:00.000Z');
  assert.match(result.events[0].eventDigest, /^[0-9a-f]{64}$/);
  assert.match(result.intentDigest, /^[0-9a-f]{64}$/);
});

test('event exactly at the end belongs to the next window', () => {
  const result = normalizeBatch(batch([event({ eventTime: '2026-08-19T10:01:00.000Z' })]));
  assert.equal(result.events[0].windowStart, '2026-08-19T10:01:00.000Z');
  assert.equal(result.events[0].windowEnd, '2026-08-19T10:02:00.000Z');
});

test('zero-event batch is allowed for explicit quiet-partition watermark progress', () => {
  const result = normalizeBatch(batch([]));
  assert.deepEqual(result.events, []);
});

test('one batch cannot repeat an event identity', () => {
  assert.throws(() => normalizeBatch(batch([event(), event()])), { code: 'invalid_request' });
  assert.throws(() => normalizeBatch(batch([
    event(),
    event({ adId: 'ad-002' }),
  ])), { code: 'conflict' });
});

test('batch rejects unknown keys, non-canonical time, and unbounded device dimension', () => {
  assert.throws(() => normalizeBatch({ ...batch(), extra: true }), { code: 'invalid_request' });
  assert.throws(() => normalizeBatch(batch([event({ eventTime: '2026-08-19T10:00:00Z' })])), {
    code: 'invalid_request',
  });
  assert.throws(() => normalizeBatch(batch([event({ device: 'smart-fridge' })])), { code: 'invalid_request' });
});

test('count and top queries have exact, mutually exclusive shapes', () => {
  const common = {
    publicationRevision: null,
    start: '2026-08-19T10:00:00.000Z',
    end: '2026-08-19T10:03:00.000Z',
    region: null,
    device: null,
    pageSize: 2,
  };
  const count = normalizeQuery({ kind: 'count', adId: 'ad-001', limit: null, ...common });
  assert.equal(count.kind, 'count');
  const top = normalizeQuery({ kind: 'top', adId: null, limit: 10, ...common });
  assert.equal(top.limit, 10);
  assert.throws(() => normalizeQuery({ kind: 'count', adId: null, limit: null, ...common }), {
    code: 'invalid_request',
  });
  assert.throws(() => normalizeQuery({
    kind: 'top',
    adId: null,
    limit: 10,
    ...common,
    end: '2026-08-19T11:01:00.000Z',
  }), { code: 'invalid_request' });
});

test('retention floor must be a canonical minute boundary', () => {
  assert.equal(normalizeRetention({
    eventTimeFloor: '2026-08-19T10:01:00.000Z',
    expectedPublicationRevision: 2,
  }).eventTimeFloor, '2026-08-19T10:01:00.000Z');
  assert.throws(() => normalizeRetention({
    eventTimeFloor: '2026-08-19T10:01:00.001Z',
    expectedPublicationRevision: 2,
  }), { code: 'invalid_request' });
});
