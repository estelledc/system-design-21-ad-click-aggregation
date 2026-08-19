import { sha256 } from './crypto.js';
import { conflict, invalid } from './errors.js';
import { minuteMs, windowFor } from './model.js';

export const labContract = Object.freeze({
  tenant: 'synthetic-lab',
  definition: 'deduplicated-click-count.v1',
  partitions: 2,
  initialWatermark: '2020-01-01T00:00:00.000Z',
  watermarkKind: 'source_declared_heuristic',
  measure: 'deduplicated_synthetic_click_count',
  version: 1,
});

export const limits = Object.freeze({
  requestBodyBytes: 256 * 1024,
  requestKeyChars: 128,
  identifierChars: 64,
  eventIdChars: 96,
  maximumEventsPerBatch: 64,
  maximumQueryWindows: 60,
  maximumPageSize: 20,
  maximumTopN: 20,
  minimumEventTime: Date.parse('2020-01-01T00:00:00.000Z'),
  maximumEventTime: Date.parse('2100-01-01T00:00:00.000Z'),
});

const identifierPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;
const devices = new Set(['desktop', 'mobile', 'tablet']);

function plainObject(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) {
    throw invalid(`${name} must be a JSON object`);
  }
  return value;
}

export function exactKeys(value, expected, name) {
  plainObject(value, name);
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) {
    throw invalid(`${name} must contain exactly: ${wanted.join(', ')}`);
  }
  return value;
}

function text(value, name, maximumBytes, pattern = identifierPattern) {
  if (typeof value !== 'string' || !value.length || Buffer.byteLength(value, 'utf8') > maximumBytes || !pattern.test(value)) {
    throw invalid(`${name} is invalid`);
  }
  return value;
}

function safeInteger(value, name, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw invalid(`${name} must be a safe integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function nullableIdentifier(value, name) {
  return value === null ? null : text(value, name, limits.identifierChars);
}

export function canonicalTimestamp(value, name = 'timestamp') {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) {
    throw invalid(`${name} must use canonical UTC millisecond format`);
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString() !== value) throw invalid(`${name} is invalid`);
  if (parsed.getTime() < limits.minimumEventTime || parsed.getTime() > limits.maximumEventTime) {
    throw invalid(`${name} is outside the lab time envelope`);
  }
  return parsed.toISOString();
}

function minuteBoundary(value, name) {
  const timestamp = canonicalTimestamp(value, name);
  if (new Date(timestamp).getTime() % minuteMs !== 0) throw invalid(`${name} must be minute-aligned`);
  return timestamp;
}

export function canonicalStringify(value) {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw invalid('canonical JSON cannot contain NaN or infinity');
    return JSON.stringify(Object.is(value, -0) ? 0 : value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalStringify).join(',')}]`;
  plainObject(value, 'canonical value');
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalStringify(value[key])}`).join(',')}}`;
}

export function digestValue(value) {
  return sha256(Buffer.from(`${canonicalStringify(value)}\n`, 'utf8'));
}

export function validateIdentifier(value, name = 'identifier') {
  return text(value, name, limits.identifierChars);
}

export function validateRequestKey(value) {
  return text(value, 'Idempotency-Key', limits.requestKeyChars, /^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$/);
}

export function normalizeSource(body) {
  exactKeys(body, ['sourceId', 'partition'], 'source body');
  return {
    sourceId: validateIdentifier(body.sourceId, 'sourceId'),
    partition: safeInteger(body.partition, 'partition', 0, labContract.partitions - 1),
  };
}

export function normalizeEpoch(body) {
  exactKeys(body, ['expectedEpoch'], 'source epoch body');
  return { expectedEpoch: safeInteger(body.expectedEpoch, 'expectedEpoch') };
}

function normalizeEvent(raw, index) {
  exactKeys(raw, ['eventId', 'eventTime', 'adId', 'region', 'device'], `events[${index}]`);
  const eventTime = canonicalTimestamp(raw.eventTime, `events[${index}].eventTime`);
  const event = {
    eventId: text(raw.eventId, `events[${index}].eventId`, limits.eventIdChars),
    eventTime,
    adId: validateIdentifier(raw.adId, `events[${index}].adId`),
    region: validateIdentifier(raw.region, `events[${index}].region`),
    device: validateIdentifier(raw.device, `events[${index}].device`),
    ...windowFor(eventTime),
  };
  if (!devices.has(event.device)) throw invalid(`events[${index}].device is not supported`);
  return { ...event, eventDigest: digestValue({
    version: labContract.version,
    tenant: labContract.tenant,
    eventTime: event.eventTime,
    adId: event.adId,
    region: event.region,
    device: event.device,
  }) };
}

export function normalizeBatch(body) {
  exactKeys(body, ['sourceId', 'sourceEpoch', 'batchSequence', 'sourceWatermark', 'events'], 'batch body');
  if (!Array.isArray(body.events) || body.events.length > limits.maximumEventsPerBatch) {
    throw invalid(`events must contain zero through ${limits.maximumEventsPerBatch} entries`);
  }
  const events = body.events.map(normalizeEvent);
  const seen = new Map();
  for (const event of events) {
    if (seen.has(event.eventId)) {
      if (seen.get(event.eventId) !== event.eventDigest) throw conflict('one batch reuses an event ID with changed intent');
      throw invalid('one batch must not repeat an event ID');
    }
    seen.set(event.eventId, event.eventDigest);
  }
  const request = {
    version: labContract.version,
    sourceId: validateIdentifier(body.sourceId, 'sourceId'),
    sourceEpoch: safeInteger(body.sourceEpoch, 'sourceEpoch'),
    batchSequence: safeInteger(body.batchSequence, 'batchSequence'),
    sourceWatermark: canonicalTimestamp(body.sourceWatermark, 'sourceWatermark'),
    events,
  };
  return { ...request, intentDigest: digestValue(request) };
}

export function normalizeAssignment(body) {
  exactKeys(body, ['partition', 'workerId', 'expectedGeneration'], 'assignment body');
  const request = {
    partition: safeInteger(body.partition, 'partition', 0, labContract.partitions - 1),
    workerId: validateIdentifier(body.workerId, 'workerId'),
    expectedGeneration: safeInteger(body.expectedGeneration, 'expectedGeneration'),
  };
  return { ...request, intentDigest: digestValue(request) };
}

export function normalizeProcess(body) {
  exactKeys(body, ['partition', 'generation', 'expectedBatchRevision'], 'process body');
  const request = {
    partition: safeInteger(body.partition, 'partition', 0, labContract.partitions - 1),
    generation: safeInteger(body.generation, 'generation', 1),
    expectedBatchRevision: safeInteger(body.expectedBatchRevision, 'expectedBatchRevision', 1),
  };
  return { ...request, intentDigest: digestValue(request) };
}

export function normalizePublication(body) {
  exactKeys(body, ['expectedPublicationRevision'], 'publication body');
  const request = {
    expectedPublicationRevision: safeInteger(body.expectedPublicationRevision, 'expectedPublicationRevision'),
  };
  return { ...request, intentDigest: digestValue(request) };
}

export function normalizeCorrection(body) {
  exactKeys(body, ['correctionId', 'eventId', 'expectedPublicationRevision'], 'correction body');
  const request = {
    correctionId: validateIdentifier(body.correctionId, 'correctionId'),
    eventId: text(body.eventId, 'eventId', limits.eventIdChars),
    expectedPublicationRevision: safeInteger(body.expectedPublicationRevision, 'expectedPublicationRevision', 1),
  };
  return { ...request, intentDigest: digestValue(request) };
}

export function normalizeRetention(body) {
  exactKeys(body, ['eventTimeFloor', 'expectedPublicationRevision'], 'retention body');
  const request = {
    eventTimeFloor: minuteBoundary(body.eventTimeFloor, 'eventTimeFloor'),
    expectedPublicationRevision: safeInteger(body.expectedPublicationRevision, 'expectedPublicationRevision', 1),
  };
  return { ...request, intentDigest: digestValue(request) };
}

export function normalizeQuery(body) {
  exactKeys(
    body,
    ['kind', 'publicationRevision', 'start', 'end', 'adId', 'region', 'device', 'limit', 'pageSize'],
    'query body',
  );
  if (body.kind !== 'count' && body.kind !== 'top') throw invalid('kind must be count or top');
  const start = minuteBoundary(body.start, 'start');
  const end = minuteBoundary(body.end, 'end');
  const windows = (new Date(end).getTime() - new Date(start).getTime()) / minuteMs;
  if (!Number.isSafeInteger(windows) || windows < 1 || windows > limits.maximumQueryWindows) {
    throw invalid(`query must span 1 through ${limits.maximumQueryWindows} minute windows`);
  }
  const query = {
    kind: body.kind,
    publicationRevision: body.publicationRevision === null
      ? null
      : safeInteger(body.publicationRevision, 'publicationRevision', 1),
    start,
    end,
    adId: nullableIdentifier(body.adId, 'adId'),
    region: nullableIdentifier(body.region, 'region'),
    device: nullableIdentifier(body.device, 'device'),
    limit: body.limit === null ? null : safeInteger(body.limit, 'limit', 1, limits.maximumTopN),
    pageSize: safeInteger(body.pageSize, 'pageSize', 1, limits.maximumPageSize),
  };
  if (query.device !== null && !devices.has(query.device)) throw invalid('device is not supported');
  if (query.kind === 'count' && (query.adId === null || query.limit !== null)) {
    throw invalid('count query requires adId and null limit');
  }
  if (query.kind === 'top' && (query.adId !== null || query.limit === null)) {
    throw invalid('top query requires null adId and integer limit');
  }
  return { ...query, intentDigest: digestValue(query) };
}
