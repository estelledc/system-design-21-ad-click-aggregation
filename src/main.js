import process from 'node:process';
import pg from 'pg';
import { createAggregationServer } from './http.js';
import { AggregationRepository, initializeDatabase } from './repository.js';
import { AggregationService } from './service.js';

const { Pool } = pg;

function log(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function databasePool() {
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
  return new Pool({ connectionString: process.env.DATABASE_URL, max: 10, statement_timeout: 10_000 });
}

async function listen(server, port) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('server did not expose a TCP listening address');
  log({ event: 'ad_click_aggregation_ready', port: address.port });
}

async function serve() {
  const pool = databasePool();
  let server;
  try {
    await initializeDatabase(pool);
    const repository = new AggregationRepository(pool);
    const service = new AggregationService(repository, log);
    server = createAggregationServer({
      service,
      repository,
      apiToken: process.env.AD_CLICK_API_TOKEN,
      logger: log,
    });
    await listen(server, Number(process.env.PORT ?? 3000));
  } catch (error) {
    if (server?.listening) await new Promise((resolve) => server.close(resolve));
    await pool.end().catch(() => {});
    throw error;
  }

  let closePromise;
  const close = async () => {
    closePromise ??= (async () => {
      await new Promise((resolve) => server.close(resolve));
      await pool.end();
    })();
    return closePromise;
  };
  process.once('SIGTERM', () => close().then(() => process.exit(0)));
  process.once('SIGINT', () => close().then(() => process.exit(0)));
}

const command = process.argv[2];
const operation = command === 'serve' ? serve() : Promise.reject(new Error('usage: node src/main.js serve'));
operation.catch((error) => {
  log({ event: 'process_failed', errorCode: error?.code ?? 'startup_error' });
  process.exitCode = 1;
});
