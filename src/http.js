import { createServer } from 'node:http';
import { equalHex, sha256 } from './crypto.js';
import { limits } from './contracts.js';
import { AggregationError, invalid, notFound, unauthorized } from './errors.js';

export async function readJson(request) {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of request) {
    bytes += chunk.length;
    if (bytes > limits.requestBodyBytes) throw invalid('request body is too large');
    chunks.push(chunk);
  }
  if (!bytes) throw invalid('JSON request body is required');
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw invalid('request body must be valid JSON');
  }
}

export function writeJson(response, status, value) {
  const bytes = Buffer.from(`${JSON.stringify(value)}\n`, 'utf8');
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-length': bytes.length,
    'content-type': 'application/json; charset=utf-8',
  });
  response.end(bytes);
}

export function authorize(request, expectedToken, principal = 'lab-operator') {
  const authorization = request.headers.authorization;
  if (typeof authorization !== 'string' || !authorization.startsWith('Bearer ')) throw unauthorized();
  const candidate = authorization.slice('Bearer '.length);
  if (!candidate.length || !equalHex(sha256(candidate), sha256(expectedToken))) throw unauthorized();
  return principal;
}

function header(request, name) {
  const value = request.headers[name.toLowerCase()];
  if (typeof value !== 'string' || !value.length) throw invalid(`${name} header is required`);
  return value;
}

function decodeSegment(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw invalid('path segment is invalid');
  }
}

export function writeError(response, error, logger) {
  const known = error instanceof AggregationError;
  logger({
    event: 'request_failed',
    errorCode: known ? error.code : 'internal_error',
    status: known ? error.status : 500,
  });
  writeJson(response, known ? error.status : 500, {
    error: known ? error.code : 'internal_error',
    message: known ? error.message : 'internal server error',
    ...(known && error.details !== undefined ? { details: error.details } : {}),
  });
}

export function createAggregationServer({ service, repository, apiToken, logger = () => {} }) {
  if (typeof apiToken !== 'string' || apiToken.length < 16) {
    throw new Error('AD_CLICK_API_TOKEN must contain at least 16 characters');
  }
  return createServer(async (request, response) => {
    const started = performance.now();
    try {
      const url = new URL(request.url, 'http://ad-click.invalid');
      if (request.method === 'GET' && url.pathname === '/health') {
        writeJson(response, 200, { status: 'ok' });
        return;
      }
      const principal = authorize(request, apiToken);
      if (request.method === 'GET' && url.pathname === '/v1/state') {
        writeJson(response, 200, await repository.getState());
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/sources') {
        const result = await service.createSource(await readJson(request));
        writeJson(response, result.created ? 201 : 200, result);
        return;
      }
      let match = url.pathname.match(/^\/v1\/sources\/([^/]+)\/epochs$/);
      if (request.method === 'POST' && match) {
        writeJson(response, 200, await service.rotateSourceEpoch({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          sourceId: decodeSegment(match[1]),
          body: await readJson(request),
        }));
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/click-batches') {
        const result = await service.ingest(await readJson(request));
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/assignments') {
        const result = await service.assign({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      match = url.pathname.match(/^\/v1\/workers\/([^/]+)\/process$/);
      if (request.method === 'POST' && match) {
        const result = await service.processBatch({
          requestKey: header(request, 'Idempotency-Key'),
          workerId: decodeSegment(match[1]),
          body: await readJson(request),
        });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/publications') {
        const result = await service.publish({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/corrections') {
        const result = await service.correct({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/retention') {
        const result = await service.retain({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/queries') {
        const result = await service.createQuery({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      match = url.pathname.match(
        /^\/v1\/queries\/([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/pages\/(\d+)$/,
      );
      if (request.method === 'GET' && match) {
        writeJson(response, 200, await repository.getQueryPage(match[1], Number(match[2])));
        return;
      }
      throw notFound();
    } catch (error) {
      writeError(response, error, logger);
    } finally {
      logger({ event: 'request_finished', durationMs: Math.round((performance.now() - started) * 1000) / 1000 });
    }
  });
}
