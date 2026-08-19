export const minuteMs = 60_000;
export const allowedLatenessMs = 2 * minuteMs;

function iso(milliseconds) {
  return new Date(milliseconds).toISOString();
}

export function windowFor(eventTime) {
  const milliseconds = new Date(eventTime).getTime();
  const start = Math.floor(milliseconds / minuteMs) * minuteMs;
  return { windowStart: iso(start), windowEnd: iso(start + minuteMs) };
}

export function finalizedThroughFor(globalWatermark) {
  const candidate = new Date(globalWatermark).getTime() - allowedLatenessMs;
  return iso(Math.floor(candidate / minuteMs) * minuteMs);
}

export function minimumWatermark(values) {
  if (!Array.isArray(values) || !values.length) throw new Error('at least one watermark is required');
  return values.reduce((minimum, value) => (value < minimum ? value : minimum));
}

export function classifyDisposition(windowEnd, finalizedThrough) {
  return finalizedThrough !== null && windowEnd <= finalizedThrough ? 'late_unapplied' : 'counted';
}

function matches(row, query) {
  return row.adId === query.adId
    && (query.region === null || row.region === query.region)
    && (query.device === null || row.device === query.device);
}

function correctedWindows(corrections, query) {
  return new Set(corrections.filter((value) => matches(value, query)).map((value) => value.windowStart));
}

export function materializeCountRows({ rows, corrections, query, finalizedThrough, retentionFloor }) {
  const corrected = correctedWindows(corrections, query);
  const output = [];
  for (let cursor = new Date(query.start).getTime(); cursor < new Date(query.end).getTime(); cursor += minuteMs) {
    const windowStart = iso(cursor);
    const windowEnd = iso(cursor + minuteMs);
    if (windowEnd > finalizedThrough) {
      output.push({ windowStart, windowEnd, status: 'not_published', count: null, rawEvidence: 'not_applicable' });
      continue;
    }
    const count = rows
      .filter((row) => row.windowStart === windowStart && matches(row, query))
      .reduce((sum, row) => sum + row.count, 0);
    output.push({
      windowStart,
      windowEnd,
      status: corrected.has(windowStart) ? 'corrected' : 'finalized',
      count,
      rawEvidence: windowEnd <= retentionFloor ? 'purged' : 'retained',
    });
  }
  return output;
}

export function materializeTopRows({ rows, query }) {
  const totals = new Map();
  for (const row of rows) {
    if (row.windowStart < query.start || row.windowStart >= query.end) continue;
    if (query.region !== null && row.region !== query.region) continue;
    if (query.device !== null && row.device !== query.device) continue;
    totals.set(row.adId, (totals.get(row.adId) ?? 0) + row.count);
  }
  return [...totals.entries()]
    .map(([adId, count]) => ({ adId, count }))
    .sort((left, right) => right.count - left.count || left.adId.localeCompare(right.adId))
    .slice(0, query.limit);
}

export function pageRows(rows, pageSize, page) {
  const pageCount = Math.max(1, Math.ceil(rows.length / pageSize));
  if (!Number.isSafeInteger(page) || page < 0 || page >= pageCount) return null;
  return { page, pageCount, rows: rows.slice(page * pageSize, (page + 1) * pageSize) };
}

export function checkConservation({ dispositions, partials, corrections = [] }) {
  const counted = dispositions.filter((value) => value === 'counted').length;
  const late = dispositions.filter((value) => value === 'late_unapplied').length;
  const base = partials.reduce((sum, value) => sum + value, 0);
  const correctionTotal = corrections.reduce((sum, value) => sum + value, 0);
  return {
    valid: counted === base && corrections.every((value) => value === 1),
    counted,
    late,
    base,
    correctionTotal,
    publishedTotal: base + correctionTotal,
  };
}
