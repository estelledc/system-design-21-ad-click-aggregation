import assert from 'node:assert/strict';
import test from 'node:test';
import { createAggregationServer } from '../../src/http.js';

const token = 'synthetic-test-token-1234';

async function fixture() {
  const calls = [];
  const repository = {
    async getState() { return { latestPublicationRevision: 0 }; },
    async getQueryPage(queryId, page) { return { queryId, page, rows: [] }; },
  };
  const service = {
    async createSource(body) { calls.push(['source', body]); return { created: true, partition: body.partition }; },
    async ingest(body) { calls.push(['ingest', body]); return { replayed: false, submittedCount: body.events.length }; },
    async assign(value) { calls.push(['assign', value]); return { replayed: false, partition: value.body.partition }; },
  };
  const logs = [];
  const server = createAggregationServer({ service, repository, apiToken: token, logger: (value) => logs.push(value) });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  return {
    calls,
    logs,
    baseUrl: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

test('health is public while state requires a bearer token', async () => {
  const app = await fixture();
  try {
    const health = await fetch(`${app.baseUrl}/health`);
    assert.equal(health.status, 200);
    assert.deepEqual(await health.json(), { status: 'ok' });
    const denied = await fetch(`${app.baseUrl}/v1/state`);
    assert.equal(denied.status, 401);
    assert.equal((await denied.json()).error, 'unauthorized');
    const state = await fetch(`${app.baseUrl}/v1/state`, {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(state.status, 200);
  } finally {
    await app.close();
  }
});

test('source and assignment routes preserve body and idempotency header', async () => {
  const app = await fixture();
  try {
    const source = await fetch(`${app.baseUrl}/v1/sources`, {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ sourceId: 'source-a', partition: 0 }),
    });
    assert.equal(source.status, 201);
    const assignment = await fetch(`${app.baseUrl}/v1/assignments`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
        'idempotency-key': 'assign-request-0001',
      },
      body: JSON.stringify({ partition: 0, workerId: 'worker-a', expectedGeneration: 0 }),
    });
    assert.equal(assignment.status, 201);
    assert.equal(app.calls[1][1].requestKey, 'assign-request-0001');
  } finally {
    await app.close();
  }
});

test('query page path is bounded and request logs omit path identities', async () => {
  const app = await fixture();
  try {
    const queryId = '00000000-0000-4000-8000-000000000001';
    const response = await fetch(`${app.baseUrl}/v1/queries/${queryId}/pages/0`, {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(response.status, 200);
    assert.equal((await response.json()).queryId, queryId);
    assert.ok(app.logs.every((entry) => !JSON.stringify(entry).includes(queryId)));
  } finally {
    await app.close();
  }
});
