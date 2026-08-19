import process from 'node:process';
import {
  digestValue,
  normalizeAssignment,
  normalizeBatch,
  normalizeCorrection,
  normalizeEpoch,
  normalizeProcess,
  normalizePublication,
  normalizeQuery,
  normalizeRetention,
  normalizeSource,
  validateIdentifier,
  validateRequestKey,
} from './contracts.js';

function crashIf(name) {
  if (process.env[name] === '1') process.kill(process.pid, 'SIGKILL');
}

export class AggregationService {
  constructor(repository, logger = () => {}) {
    this.repository = repository;
    this.logger = logger;
  }

  async createSource(body) {
    const result = await this.repository.createSource(normalizeSource(body));
    this.logger({ event: 'source_registered', created: result.created, partition: result.partition });
    return result;
  }

  async rotateSourceEpoch({ principal, requestKey, sourceId, body }) {
    const source = validateIdentifier(sourceId, 'sourceId');
    const request = normalizeEpoch(body);
    const result = await this.repository.rotateSourceEpoch({
      principal,
      requestKey: validateRequestKey(requestKey),
      sourceId: source,
      ...request,
      intentDigest: digestValue({ operation: 'rotate_epoch', sourceId: source, ...request }),
    });
    this.logger({ event: 'source_epoch_rotated', replayed: result.replayed });
    return result;
  }

  async ingest(body) {
    const result = await this.repository.ingest(normalizeBatch(body));
    this.logger({
      event: 'batch_ingested',
      replayed: result.replayed,
      submittedCount: result.submittedCount,
      newCount: result.newCount,
      duplicateCount: result.duplicateCount,
    });
    crashIf('AD_CLICK_CRASH_AFTER_INGEST_COMMIT');
    return result;
  }

  async assign({ principal, requestKey, body }) {
    const request = normalizeAssignment(body);
    const result = await this.repository.assign({
      principal,
      requestKey: validateRequestKey(requestKey),
      ...request,
    });
    this.logger({ event: 'assignment_created', partition: result.partition, replayed: result.replayed });
    return result;
  }

  async processBatch({ requestKey, workerId, body }) {
    const worker = validateIdentifier(workerId, 'workerId');
    const request = normalizeProcess(body);
    const result = await this.repository.processBatch({
      requestKey: validateRequestKey(requestKey),
      workerId: worker,
      ...request,
      intentDigest: digestValue({ operation: 'process_batch', workerId: worker, ...request }),
    });
    this.logger({
      event: 'batch_processed',
      partition: result.partition,
      replayed: result.replayed,
      countedCount: result.countedCount,
      lateCount: result.lateCount,
    });
    crashIf('AD_CLICK_CRASH_AFTER_PROCESS_COMMIT');
    return result;
  }

  async publish({ principal, requestKey, body }) {
    const request = normalizePublication(body);
    const result = await this.repository.publish({
      principal,
      requestKey: validateRequestKey(requestKey),
      ...request,
    });
    this.logger({
      event: 'publication_created',
      replayed: result.replayed,
      publicationRowCount: result.publicationRowCount,
    });
    crashIf('AD_CLICK_CRASH_AFTER_PUBLICATION_COMMIT');
    return result;
  }

  async correct({ principal, requestKey, body }) {
    const request = normalizeCorrection(body);
    const result = await this.repository.correct({
      principal,
      requestKey: validateRequestKey(requestKey),
      ...request,
    });
    this.logger({ event: 'correction_applied', replayed: result.replayed, delta: result.delta });
    crashIf('AD_CLICK_CRASH_AFTER_CORRECTION_COMMIT');
    return result;
  }

  async retain({ principal, requestKey, body }) {
    const request = normalizeRetention(body);
    const result = await this.repository.retain({
      principal,
      requestKey: validateRequestKey(requestKey),
      ...request,
    });
    this.logger({
      event: 'retention_advanced',
      replayed: result.replayed,
      removedEventLedgerRows: result.removedEventLedgerRows,
    });
    return result;
  }

  async createQuery({ principal, requestKey, body }) {
    const query = normalizeQuery(body);
    const result = await this.repository.createQuery({
      principal,
      requestKey: validateRequestKey(requestKey),
      ...query,
    });
    this.logger({
      event: 'query_materialized',
      kind: result.kind,
      replayed: result.replayed,
      rowCount: result.rows.length,
      pageCount: result.pageCount,
    });
    return result;
  }
}
