export class AggregationError extends Error {
  constructor(code, status, message, details) {
    super(message);
    this.name = 'AggregationError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

export const invalid = (message = 'request is invalid') => new AggregationError('invalid_request', 400, message);
export const unauthorized = () => new AggregationError('unauthorized', 401, 'authorization failed');
export const notFound = (message = 'resource was not found') => new AggregationError('not_found', 404, message);
export const conflict = (message = 'stable identity conflicts with prior intent') => (
  new AggregationError('conflict', 409, message)
);
export const outOfOrder = (kind, expected) => new AggregationError(
  'out_of_order',
  409,
  `${kind} is out of order`,
  { expected },
);
export const fenced = (kind, current) => new AggregationError(
  'fenced',
  409,
  `${kind} is stale`,
  { current },
);
export const noWork = () => new AggregationError('no_work', 409, 'requested source batch is not durable');
export const notPublishable = () => new AggregationError(
  'not_publishable',
  409,
  'global watermark does not advance the published cutoff',
);
export const windowNotPublished = () => new AggregationError(
  'window_not_published',
  409,
  'query range is not fully covered by the selected publication',
);
export const lateEventRequired = () => new AggregationError(
  'late_event_required',
  409,
  'correction target is not an unapplied late event',
);
export const beyondDedupHorizon = () => new AggregationError(
  'beyond_dedup_horizon',
  409,
  'event time precedes the retained identity horizon',
);
export const retentionBlocked = (reason) => new AggregationError(
  'retention_blocked',
  409,
  'retention precondition failed',
  { reason },
);
