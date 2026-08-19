import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { labContract } from './contracts.js';
import {
  beyondDedupHorizon,
  conflict,
  fenced,
  lateEventRequired,
  noWork,
  notFound,
  notPublishable,
  outOfOrder,
  retentionBlocked,
  windowNotPublished,
} from './errors.js';
import {
  classifyDisposition,
  finalizedThroughFor,
  materializeCountRows,
  materializeTopRows,
  minimumWatermark,
  pageRows,
} from './model.js';

const schemaUrl = new URL('../sql/schema.sql', import.meta.url);

function integer(value) {
  const result = Number(value);
  if (!Number.isSafeInteger(result)) throw new Error('database integer exceeded the safe range');
  return result;
}

function timestamp(value) {
  return new Date(value).toISOString();
}

async function rollback(client) {
  await client.query('ROLLBACK').catch(() => {});
}

export async function initializeDatabase(pool) {
  await pool.query(await readFile(fileURLToPath(schemaUrl), 'utf8'));
}

export async function resetDatabase(pool, confirmation) {
  if (confirmation !== 'ad-click-aggregation-lab-reset') {
    throw new Error('refusing to reset without the explicit lab confirmation');
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`
      TRUNCATE aca_queries, aca_corrections, aca_publication_rows, aca_publications,
        aca_window_partials, aca_event_dispositions, aca_process_receipts,
        aca_mutation_receipts, aca_assignment_history, aca_batch_events,
        aca_events, aca_batches, aca_sources, aca_assignments, aca_state CASCADE
    `);
    await client.query(`
      INSERT INTO aca_state (
        singleton, ingest_revision, next_publication_revision, latest_publication_revision,
        correction_generation, retention_event_time_floor, updated_at
      ) VALUES (true, 0, 1, NULL, 0, $1, clock_timestamp())
    `, [labContract.initialWatermark]);
    await client.query(`
      INSERT INTO aca_assignments (
        partition_id, current_generation, current_worker_id,
        checkpoint_batch_revision, processed_watermark, updated_at
      ) VALUES
        (0, 0, NULL, 0, $1, clock_timestamp()),
        (1, 0, NULL, 0, $1, clock_timestamp())
    `, [labContract.initialWatermark]);
    await client.query('COMMIT');
  } catch (error) {
    await rollback(client);
    throw error;
  } finally {
    client.release();
  }
}

function sourceResult(row, created) {
  return {
    sourceId: row.source_id,
    partition: integer(row.partition_id),
    sourceEpoch: integer(row.source_epoch),
    nextSequence: integer(row.next_sequence),
    nextBatchRevision: integer(row.next_batch_revision),
    nextInputOffset: integer(row.next_input_offset),
    sourceWatermark: timestamp(row.latest_source_watermark),
    created,
  };
}

export class AggregationRepository {
  constructor(pool) {
    this.pool = pool;
  }

  async #databaseNow(client) {
    const result = await client.query(`SELECT clock_timestamp() AS now`);
    return new Date(result.rows[0].now);
  }

  async #advisory(client, value) {
    await client.query(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [value]);
  }

  async #lockState(client) {
    const result = await client.query(`SELECT * FROM aca_state WHERE singleton = true FOR UPDATE`);
    if (!result.rowCount) throw new Error('aggregation state is missing');
    return result.rows[0];
  }

  async #existingMutation(client, principal, requestKey, intentDigest) {
    const found = await client.query(
      `SELECT * FROM aca_mutation_receipts WHERE principal = $1 AND request_key = $2`,
      [principal, requestKey],
    );
    if (!found.rowCount) return null;
    if (found.rows[0].intent_digest !== intentDigest) throw conflict();
    return { ...found.rows[0].result_json, replayed: true };
  }

  async #recordMutation(client, { principal, requestKey, intentDigest, operation, result, now }) {
    await client.query(`
      INSERT INTO aca_mutation_receipts (
        principal, request_key, intent_digest, operation, result_json, created_at
      ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
    `, [principal, requestKey, intentDigest, operation, JSON.stringify(result), now]);
    return { ...result, replayed: false };
  }

  async #beginMutation(client, principal, requestKey, intentDigest) {
    await this.#advisory(client, `mutation:${principal.length}:${principal}:${requestKey.length}:${requestKey}`);
    return this.#existingMutation(client, principal, requestKey, intentDigest);
  }

  async #materializePublication(client, revision, finalizedThrough) {
    const inserted = await client.query(`
      WITH values_by_key AS (
        SELECT window_start, window_end, ad_id, region, device, count
        FROM aca_window_partials
        WHERE definition_id = $2 AND window_end <= $3
        UNION ALL
        SELECT disposition.window_start, disposition.window_end,
          disposition.ad_id, disposition.region, disposition.device,
          correction.delta::bigint AS count
        FROM aca_corrections AS correction
        JOIN aca_event_dispositions AS disposition
          ON disposition.definition_id = $2 AND disposition.event_id = correction.event_id
        WHERE correction.applied_publication_revision <= $1
          AND disposition.window_end <= $3
      )
      INSERT INTO aca_publication_rows (
        publication_revision, window_start, window_end, ad_id, region, device, count
      )
      SELECT $1, window_start, window_end, ad_id, region, device, sum(count)::bigint
      FROM values_by_key
      GROUP BY window_start, window_end, ad_id, region, device
      HAVING sum(count) > 0
    `, [revision, labContract.definition, finalizedThrough]);
    return inserted.rowCount;
  }

  async getState() {
    const stateResult = await this.pool.query(`
      SELECT state.*,
        publication.finalized_through,
        publication.global_watermark,
        (SELECT count(*) FROM aca_sources) AS source_count,
        (SELECT count(*) FROM aca_batches) AS batch_count,
        (SELECT count(*) FROM aca_events) AS retained_event_count,
        (SELECT count(*) FROM aca_event_dispositions WHERE disposition = 'counted') AS counted_event_count,
        (SELECT count(*) FROM aca_event_dispositions AS disposition
          WHERE disposition.disposition = 'late_unapplied'
            AND NOT EXISTS (SELECT 1 FROM aca_corrections AS correction WHERE correction.event_id = disposition.event_id)
        ) AS pending_late_count,
        (SELECT count(*) FROM aca_corrections) AS correction_count,
        (SELECT count(*) FROM aca_queries) AS query_count
      FROM aca_state AS state
      LEFT JOIN aca_publications AS publication
        ON publication.publication_revision = state.latest_publication_revision
      WHERE state.singleton = true
    `);
    const partitionsResult = await this.pool.query(`
      SELECT assignment.partition_id, assignment.current_generation,
        assignment.checkpoint_batch_revision, assignment.processed_watermark,
        COALESCE(source.next_batch_revision - 1, 0) AS latest_durable_batch_revision,
        COALESCE(source.latest_source_watermark, $1::timestamptz) AS source_watermark
      FROM aca_assignments AS assignment
      LEFT JOIN aca_sources AS source ON source.partition_id = assignment.partition_id
      ORDER BY assignment.partition_id
    `, [labContract.initialWatermark]);
    const row = stateResult.rows[0];
    return {
      ingestRevision: integer(row.ingest_revision),
      latestPublicationRevision: row.latest_publication_revision === null ? 0 : integer(row.latest_publication_revision),
      correctionGeneration: integer(row.correction_generation),
      retentionEventTimeFloor: timestamp(row.retention_event_time_floor),
      globalWatermark: row.global_watermark === null ? null : timestamp(row.global_watermark),
      finalizedThrough: row.finalized_through === null ? null : timestamp(row.finalized_through),
      sourceCount: integer(row.source_count),
      batchCount: integer(row.batch_count),
      retainedEventCount: integer(row.retained_event_count),
      countedEventCount: integer(row.counted_event_count),
      pendingLateCount: integer(row.pending_late_count),
      correctionCount: integer(row.correction_count),
      queryCount: integer(row.query_count),
      watermarkKind: labContract.watermarkKind,
      completenessProved: false,
      partitions: partitionsResult.rows.map((partition) => ({
        partition: integer(partition.partition_id),
        assignmentGeneration: integer(partition.current_generation),
        checkpointBatchRevision: integer(partition.checkpoint_batch_revision),
        latestDurableBatchRevision: integer(partition.latest_durable_batch_revision),
        processedWatermark: timestamp(partition.processed_watermark),
        sourceWatermark: timestamp(partition.source_watermark),
      })),
    };
  }

  async createSource({ sourceId, partition }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `source-create:${sourceId.length}:${sourceId}`);
      const existing = await client.query(`SELECT * FROM aca_sources WHERE source_id = $1`, [sourceId]);
      if (existing.rowCount) {
        if (integer(existing.rows[0].partition_id) !== partition) throw conflict('source partition is immutable');
        await client.query('COMMIT');
        return sourceResult(existing.rows[0], false);
      }
      const occupied = await client.query(`SELECT 1 FROM aca_sources WHERE partition_id = $1`, [partition]);
      if (occupied.rowCount) throw conflict('partition already has a source');
      const now = await this.#databaseNow(client);
      const inserted = await client.query(`
        INSERT INTO aca_sources (
          source_id, partition_id, source_epoch, next_sequence, next_batch_revision,
          next_input_offset, latest_source_watermark, created_at, updated_at
        ) VALUES ($1, $2, 0, 0, 1, 0, $3, $4, $4)
        RETURNING *
      `, [sourceId, partition, labContract.initialWatermark, now]);
      await client.query('COMMIT');
      return sourceResult(inserted.rows[0], true);
    } catch (error) {
      await rollback(client);
      if (error.code === '23505') throw conflict('source or partition already exists');
      throw error;
    } finally {
      client.release();
    }
  }

  async rotateSourceEpoch({ principal, requestKey, intentDigest, sourceId, expectedEpoch }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      let result = await this.#beginMutation(client, principal, requestKey, intentDigest);
      if (result) {
        await client.query('COMMIT');
        return result;
      }
      const source = await client.query(`SELECT * FROM aca_sources WHERE source_id = $1 FOR UPDATE`, [sourceId]);
      if (!source.rowCount) throw notFound('source was not found');
      const current = integer(source.rows[0].source_epoch);
      if (current !== expectedEpoch) throw fenced('source epoch', current);
      const now = await this.#databaseNow(client);
      const nextEpoch = current + 1;
      await client.query(`
        UPDATE aca_sources SET source_epoch = $2, next_sequence = 0, updated_at = $3 WHERE source_id = $1
      `, [sourceId, nextEpoch, now]);
      result = await this.#recordMutation(client, {
        principal,
        requestKey,
        intentDigest,
        operation: 'rotate_epoch',
        result: {
          operation: 'rotate_epoch',
          sourceId,
          sourceEpoch: nextEpoch,
          nextSequence: 0,
          nextBatchRevision: integer(source.rows[0].next_batch_revision),
          nextInputOffset: integer(source.rows[0].next_input_offset),
          sourceWatermark: timestamp(source.rows[0].latest_source_watermark),
        },
        now,
      });
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async ingest(request) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const state = await this.#lockState(client);
      const sourceResultSet = await client.query(`SELECT * FROM aca_sources WHERE source_id = $1 FOR UPDATE`, [request.sourceId]);
      if (!sourceResultSet.rowCount) throw notFound('source was not found');
      const existing = await client.query(`
        SELECT result_json, intent_digest FROM aca_batches
        WHERE source_id = $1 AND source_epoch = $2 AND batch_sequence = $3
      `, [request.sourceId, request.sourceEpoch, request.batchSequence]);
      if (existing.rowCount) {
        if (existing.rows[0].intent_digest !== request.intentDigest) throw conflict();
        await client.query('COMMIT');
        return { ...existing.rows[0].result_json, replayed: true };
      }

      const source = sourceResultSet.rows[0];
      const currentEpoch = integer(source.source_epoch);
      if (currentEpoch !== request.sourceEpoch) throw fenced('source epoch', currentEpoch);
      const nextSequence = integer(source.next_sequence);
      if (request.batchSequence !== nextSequence) throw outOfOrder('source sequence', nextSequence);
      if (request.sourceWatermark < timestamp(source.latest_source_watermark)) {
        throw outOfOrder('source watermark', timestamp(source.latest_source_watermark));
      }
      const retentionFloor = timestamp(state.retention_event_time_floor);
      if (request.events.some((event) => event.eventTime < retentionFloor)) throw beyondDedupHorizon();

      for (const eventId of [...request.events.map((event) => event.eventId)].sort()) {
        await this.#advisory(client, `event:${eventId.length}:${eventId}`);
      }
      const eventIds = request.events.map((event) => event.eventId);
      const known = eventIds.length
        ? await client.query(`SELECT event_id, event_digest FROM aca_events WHERE event_id = ANY($1::text[])`, [eventIds])
        : { rows: [] };
      const knownById = new Map(known.rows.map((row) => [row.event_id, row.event_digest]));
      for (const event of request.events) {
        const prior = knownById.get(event.eventId);
        if (prior !== undefined && prior !== event.eventDigest) throw conflict('event ID binds changed intent');
      }

      const newEvents = request.events.filter((event) => !knownById.has(event.eventId));
      const batchId = randomUUID();
      const batchRevision = integer(source.next_batch_revision);
      const inputStart = integer(source.next_input_offset);
      const inputEnd = inputStart + newEvents.length;
      const ingestRevision = integer(state.ingest_revision) + 1;
      const now = await this.#databaseNow(client);
      const result = {
        batchId,
        sourceId: request.sourceId,
        sourceEpoch: request.sourceEpoch,
        batchSequence: request.batchSequence,
        batchRevision,
        inputStartOffset: inputStart,
        inputEndOffset: inputEnd,
        submittedCount: request.events.length,
        newCount: newEvents.length,
        duplicateCount: request.events.length - newEvents.length,
        sourceWatermark: request.sourceWatermark,
        durable: true,
        aggregateVisible: false,
      };
      await client.query(`
        INSERT INTO aca_batches (
          batch_id, source_id, source_epoch, batch_sequence, batch_revision, intent_digest,
          source_watermark, input_start_offset, input_end_offset,
          submitted_count, new_count, duplicate_count, result_json, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14)
      `, [
        batchId,
        request.sourceId,
        request.sourceEpoch,
        request.batchSequence,
        batchRevision,
        request.intentDigest,
        request.sourceWatermark,
        inputStart,
        inputEnd,
        request.events.length,
        newEvents.length,
        request.events.length - newEvents.length,
        JSON.stringify(result),
        now,
      ]);
      const offsetById = new Map();
      for (const [index, event] of newEvents.entries()) {
        const inputOffset = inputStart + index;
        offsetById.set(event.eventId, inputOffset);
        await client.query(`
          INSERT INTO aca_events (
            event_id, event_digest, owner_batch_id, source_id, partition_id, input_offset,
            event_time, window_start, window_end, ad_id, region, device, ingest_revision, created_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        `, [
          event.eventId,
          event.eventDigest,
          batchId,
          request.sourceId,
          integer(source.partition_id),
          inputOffset,
          event.eventTime,
          event.windowStart,
          event.windowEnd,
          event.adId,
          event.region,
          event.device,
          ingestRevision,
          now,
        ]);
      }
      for (const [ordinal, event] of request.events.entries()) {
        await client.query(`
          INSERT INTO aca_batch_events (batch_id, ordinal, event_id, owned)
          VALUES ($1, $2, $3, $4)
        `, [batchId, ordinal, event.eventId, offsetById.has(event.eventId)]);
      }
      await client.query(`
        UPDATE aca_sources
        SET next_sequence = $2, next_batch_revision = $3, next_input_offset = $4,
          latest_source_watermark = $5, updated_at = $6
        WHERE source_id = $1
      `, [request.sourceId, nextSequence + 1, batchRevision + 1, inputEnd, request.sourceWatermark, now]);
      await client.query(`
        UPDATE aca_state SET ingest_revision = $1, updated_at = $2 WHERE singleton = true
      `, [ingestRevision, now]);
      await client.query('COMMIT');
      return { ...result, replayed: false };
    } catch (error) {
      await rollback(client);
      if (error.code === '23505') throw conflict();
      throw error;
    } finally {
      client.release();
    }
  }

  async assign({ principal, requestKey, ...request }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      let result = await this.#beginMutation(client, principal, requestKey, request.intentDigest);
      if (result) {
        await client.query('COMMIT');
        return result;
      }
      const assignment = await client.query(
        `SELECT * FROM aca_assignments WHERE partition_id = $1 FOR UPDATE`,
        [request.partition],
      );
      const current = integer(assignment.rows[0].current_generation);
      if (current !== request.expectedGeneration) throw fenced('assignment generation', current);
      const now = await this.#databaseNow(client);
      if (current > 0) {
        await client.query(`
          UPDATE aca_assignment_history
          SET status = 'revoked', revoked_at = $3
          WHERE partition_id = $1 AND generation = $2 AND status = 'current'
        `, [request.partition, current, now]);
      }
      const generation = current + 1;
      await client.query(`
        INSERT INTO aca_assignment_history (
          partition_id, generation, worker_id, status, assigned_at
        ) VALUES ($1, $2, $3, 'current', $4)
      `, [request.partition, generation, request.workerId, now]);
      await client.query(`
        UPDATE aca_assignments
        SET current_generation = $2, current_worker_id = $3, updated_at = $4
        WHERE partition_id = $1
      `, [request.partition, generation, request.workerId, now]);
      result = await this.#recordMutation(client, {
        principal,
        requestKey,
        intentDigest: request.intentDigest,
        operation: 'assign',
        result: {
          operation: 'assign',
          partition: request.partition,
          workerId: request.workerId,
          generation,
          checkpointBatchRevision: integer(assignment.rows[0].checkpoint_batch_revision),
          processedWatermark: timestamp(assignment.rows[0].processed_watermark),
        },
        now,
      });
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatch({ requestKey, workerId, ...request }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(
        client,
        `process:${request.partition}:${request.generation}:${requestKey.length}:${requestKey}`,
      );
      const existing = await client.query(`
        SELECT intent_digest, result_json FROM aca_process_receipts
        WHERE partition_id = $1 AND generation = $2 AND request_key = $3
      `, [request.partition, request.generation, requestKey]);
      if (existing.rowCount) {
        if (existing.rows[0].intent_digest !== request.intentDigest) throw conflict();
        await client.query('COMMIT');
        return { ...existing.rows[0].result_json, replayed: true };
      }

      const state = await this.#lockState(client);
      const assignmentResult = await client.query(
        `SELECT * FROM aca_assignments WHERE partition_id = $1 FOR UPDATE`,
        [request.partition],
      );
      const assignment = assignmentResult.rows[0];
      const generation = integer(assignment.current_generation);
      if (generation !== request.generation || assignment.current_worker_id !== workerId) {
        throw fenced('assignment generation', generation);
      }
      const expected = integer(assignment.checkpoint_batch_revision) + 1;
      if (request.expectedBatchRevision !== expected) throw outOfOrder('batch revision', expected);
      const batchResult = await client.query(`
        SELECT batch.*
        FROM aca_batches AS batch
        JOIN aca_sources AS source ON source.source_id = batch.source_id
        WHERE source.partition_id = $1 AND batch.batch_revision = $2
        FOR SHARE OF batch
      `, [request.partition, request.expectedBatchRevision]);
      if (!batchResult.rowCount) throw noWork();
      const batch = batchResult.rows[0];
      const events = await client.query(`
        SELECT mapping.owned, event.*
        FROM aca_batch_events AS mapping
        LEFT JOIN aca_events AS event ON event.event_id = mapping.event_id
        WHERE mapping.batch_id = $1
        ORDER BY mapping.ordinal
      `, [batch.batch_id]);
      const latestPublication = state.latest_publication_revision === null
        ? null
        : await client.query(`
          SELECT finalized_through FROM aca_publications WHERE publication_revision = $1
        `, [state.latest_publication_revision]);
      const finalizedThrough = latestPublication === null ? null : timestamp(latestPublication.rows[0].finalized_through);
      let counted = 0;
      let late = 0;
      let owned = 0;
      const now = await this.#databaseNow(client);
      for (const row of events.rows) {
        if (!row.owned) continue;
        if (row.event_id === null) throw new Error('unprocessed event was removed before its checkpoint');
        owned += 1;
        const disposition = classifyDisposition(timestamp(row.window_end), finalizedThrough);
        await client.query(`
          INSERT INTO aca_event_dispositions (
            definition_id, event_id, batch_id, partition_id, batch_revision,
            assignment_generation, event_time, window_start, window_end,
            ad_id, region, device, disposition, created_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        `, [
          labContract.definition,
          row.event_id,
          batch.batch_id,
          request.partition,
          request.expectedBatchRevision,
          request.generation,
          row.event_time,
          row.window_start,
          row.window_end,
          row.ad_id,
          row.region,
          row.device,
          disposition,
          now,
        ]);
        if (disposition === 'counted') {
          counted += 1;
          await client.query(`
            INSERT INTO aca_window_partials (
              definition_id, window_start, window_end, ad_id, region, device, count, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, 1, $7)
            ON CONFLICT (definition_id, window_start, ad_id, region, device)
            DO UPDATE SET count = aca_window_partials.count + 1, updated_at = EXCLUDED.updated_at
          `, [labContract.definition, row.window_start, row.window_end, row.ad_id, row.region, row.device, now]);
        } else {
          late += 1;
        }
      }
      const batchWatermark = timestamp(batch.source_watermark);
      if (batchWatermark < timestamp(assignment.processed_watermark)) {
        throw new Error('processed watermark would move backward');
      }
      await client.query(`
        UPDATE aca_assignments
        SET checkpoint_batch_revision = $2, processed_watermark = $3, updated_at = $4
        WHERE partition_id = $1
      `, [request.partition, request.expectedBatchRevision, batchWatermark, now]);
      const result = {
        operation: 'process_batch',
        partition: request.partition,
        generation: request.generation,
        batchRevision: request.expectedBatchRevision,
        ownedCount: owned,
        duplicateReferenceCount: integer(batch.duplicate_count),
        countedCount: counted,
        lateCount: late,
        checkpointBatchRevision: request.expectedBatchRevision,
        processedWatermark: batchWatermark,
        contributionAndCheckpointAtomic: true,
      };
      await client.query(`
        INSERT INTO aca_process_receipts (
          partition_id, generation, request_key, intent_digest,
          batch_revision, result_json, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
      `, [
        request.partition,
        request.generation,
        requestKey,
        request.intentDigest,
        request.expectedBatchRevision,
        JSON.stringify(result),
        now,
      ]);
      await client.query('COMMIT');
      return { ...result, replayed: false };
    } catch (error) {
      await rollback(client);
      if (error.code === '23505') throw conflict('event already has a base disposition');
      throw error;
    } finally {
      client.release();
    }
  }

  async publish({ principal, requestKey, ...request }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      let result = await this.#beginMutation(client, principal, requestKey, request.intentDigest);
      if (result) {
        await client.query('COMMIT');
        return result;
      }
      const state = await this.#lockState(client);
      const currentRevision = state.latest_publication_revision === null ? 0 : integer(state.latest_publication_revision);
      if (currentRevision !== request.expectedPublicationRevision) {
        throw fenced('publication revision', currentRevision);
      }
      const sourceCount = await client.query(`SELECT count(*) AS count FROM aca_sources`);
      if (integer(sourceCount.rows[0].count) !== labContract.partitions) throw notPublishable();
      const assignments = await client.query(`
        SELECT * FROM aca_assignments ORDER BY partition_id FOR UPDATE
      `);
      const globalWatermark = minimumWatermark(assignments.rows.map((row) => timestamp(row.processed_watermark)));
      const finalizedThrough = finalizedThroughFor(globalWatermark);
      const previous = currentRevision === 0
        ? null
        : await client.query(`SELECT finalized_through FROM aca_publications WHERE publication_revision = $1`, [currentRevision]);
      const previousCutoff = previous === null ? timestamp(state.retention_event_time_floor) : timestamp(previous.rows[0].finalized_through);
      if (finalizedThrough <= previousCutoff) throw notPublishable();
      const revision = integer(state.next_publication_revision);
      const correctionGeneration = integer(state.correction_generation);
      const now = await this.#databaseNow(client);
      await client.query(`
        INSERT INTO aca_publications (
          publication_revision, kind, global_watermark, finalized_through,
          correction_generation, retention_event_time_floor, created_at
        ) VALUES ($1, 'finalization', $2, $3, $4, $5, $6)
      `, [revision, globalWatermark, finalizedThrough, correctionGeneration, state.retention_event_time_floor, now]);
      const rowCount = await this.#materializePublication(client, revision, finalizedThrough);
      await client.query(`
        UPDATE aca_state
        SET next_publication_revision = $1, latest_publication_revision = $2, updated_at = $3
        WHERE singleton = true
      `, [revision + 1, revision, now]);
      result = await this.#recordMutation(client, {
        principal,
        requestKey,
        intentDigest: request.intentDigest,
        operation: 'publish',
        result: {
          operation: 'publish',
          publicationRevision: revision,
          correctionGeneration,
          globalWatermark,
          finalizedThrough,
          publicationRowCount: rowCount,
          watermarkKind: labContract.watermarkKind,
          finalizedByPolicy: true,
          completenessProved: false,
        },
        now,
      });
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async correct({ principal, requestKey, ...request }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `correction:${request.correctionId.length}:${request.correctionId}`);
      await this.#advisory(client, `correction-request:${principal.length}:${principal}:${requestKey.length}:${requestKey}`);
      const byRequest = await client.query(`
        SELECT * FROM aca_corrections WHERE principal = $1 AND request_key = $2
      `, [principal, requestKey]);
      const byId = byRequest.rowCount ? byRequest : await client.query(`
        SELECT * FROM aca_corrections WHERE correction_id = $1
      `, [request.correctionId]);
      if (byId.rowCount) {
        if (byId.rows[0].intent_digest !== request.intentDigest) throw conflict();
        await client.query('COMMIT');
        return { ...byId.rows[0].result_json, replayed: true };
      }
      const state = await this.#lockState(client);
      const currentRevision = state.latest_publication_revision === null ? 0 : integer(state.latest_publication_revision);
      if (currentRevision !== request.expectedPublicationRevision) {
        throw fenced('publication revision', currentRevision);
      }
      const disposition = await client.query(`
        SELECT * FROM aca_event_dispositions
        WHERE definition_id = $1 AND event_id = $2
        FOR UPDATE
      `, [labContract.definition, request.eventId]);
      if (!disposition.rowCount || disposition.rows[0].disposition !== 'late_unapplied') throw lateEventRequired();
      const prior = await client.query(`SELECT 1 FROM aca_corrections WHERE event_id = $1`, [request.eventId]);
      if (prior.rowCount) throw conflict('late event already has a correction');
      const publication = await client.query(`
        SELECT * FROM aca_publications WHERE publication_revision = $1
      `, [currentRevision]);
      if (!publication.rowCount) throw notFound('publication was not found');
      if (timestamp(disposition.rows[0].window_end) > timestamp(publication.rows[0].finalized_through)) {
        throw lateEventRequired();
      }
      const revision = integer(state.next_publication_revision);
      const correctionGeneration = integer(state.correction_generation) + 1;
      const now = await this.#databaseNow(client);
      await client.query(`
        INSERT INTO aca_publications (
          publication_revision, kind, global_watermark, finalized_through,
          correction_generation, retention_event_time_floor, created_at
        ) VALUES ($1, 'correction', $2, $3, $4, $5, $6)
      `, [
        revision,
        publication.rows[0].global_watermark,
        publication.rows[0].finalized_through,
        correctionGeneration,
        state.retention_event_time_floor,
        now,
      ]);
      await client.query(`
        INSERT INTO aca_corrections (
          correction_id, principal, request_key, intent_digest, event_id,
          delta, correction_generation, applied_publication_revision, result_json, created_at
        ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, '{}'::jsonb, $8)
      `, [
        request.correctionId,
        principal,
        requestKey,
        request.intentDigest,
        request.eventId,
        correctionGeneration,
        revision,
        now,
      ]);
      const rowCount = await this.#materializePublication(
        client,
        revision,
        timestamp(publication.rows[0].finalized_through),
      );
      const result = {
        operation: 'correct',
        correctionId: request.correctionId,
        correctionGeneration,
        publicationRevision: revision,
        priorPublicationRevision: currentRevision,
        delta: 1,
        publicationRowCount: rowCount,
        aggregateOnly: true,
        billingProved: false,
        chargeProved: false,
      };
      await client.query(`
        UPDATE aca_corrections SET result_json = $2::jsonb WHERE correction_id = $1
      `, [request.correctionId, JSON.stringify(result)]);
      await client.query(`
        UPDATE aca_state
        SET next_publication_revision = $1, latest_publication_revision = $2,
          correction_generation = $3, updated_at = $4
        WHERE singleton = true
      `, [revision + 1, revision, correctionGeneration, now]);
      await client.query('COMMIT');
      return { ...result, replayed: false };
    } catch (error) {
      await rollback(client);
      if (error.code === '23505') throw conflict();
      throw error;
    } finally {
      client.release();
    }
  }

  async retain({ principal, requestKey, ...request }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      let result = await this.#beginMutation(client, principal, requestKey, request.intentDigest);
      if (result) {
        await client.query('COMMIT');
        return result;
      }
      const state = await this.#lockState(client);
      const currentRevision = state.latest_publication_revision === null ? 0 : integer(state.latest_publication_revision);
      if (currentRevision !== request.expectedPublicationRevision) {
        throw fenced('publication revision', currentRevision);
      }
      const currentFloor = timestamp(state.retention_event_time_floor);
      if (request.eventTimeFloor <= currentFloor) throw retentionBlocked('floor_not_advanced');
      const publication = await client.query(`
        SELECT finalized_through FROM aca_publications WHERE publication_revision = $1
      `, [currentRevision]);
      if (!publication.rowCount || request.eventTimeFloor > timestamp(publication.rows[0].finalized_through)) {
        throw retentionBlocked('floor_not_published');
      }
      const progress = await client.query(`
        SELECT count(*) FILTER (
          WHERE assignment.checkpoint_batch_revision = source.next_batch_revision - 1
        ) AS current_count, count(*) AS source_count
        FROM aca_sources AS source
        JOIN aca_assignments AS assignment ON assignment.partition_id = source.partition_id
      `);
      if (
        integer(progress.rows[0].source_count) !== labContract.partitions
        || integer(progress.rows[0].current_count) !== labContract.partitions
      ) throw retentionBlocked('unprocessed_batches');
      const pending = await client.query(`
        SELECT count(*) AS count
        FROM aca_event_dispositions AS disposition
        WHERE disposition.disposition = 'late_unapplied'
          AND disposition.event_time < $1
          AND NOT EXISTS (
            SELECT 1 FROM aca_corrections AS correction WHERE correction.event_id = disposition.event_id
          )
      `, [request.eventTimeFloor]);
      if (integer(pending.rows[0].count) > 0) throw retentionBlocked('unapplied_late_events');
      const removedMappings = await client.query(`
        DELETE FROM aca_batch_events AS mapping
        USING aca_events AS event
        WHERE mapping.event_id = event.event_id AND event.event_time < $1
      `, [request.eventTimeFloor]);
      const removedEvents = await client.query(`DELETE FROM aca_events WHERE event_time < $1`, [request.eventTimeFloor]);
      const now = await this.#databaseNow(client);
      await client.query(`
        UPDATE aca_state SET retention_event_time_floor = $1, updated_at = $2 WHERE singleton = true
      `, [request.eventTimeFloor, now]);
      result = await this.#recordMutation(client, {
        principal,
        requestKey,
        intentDigest: request.intentDigest,
        operation: 'retain',
        result: {
          operation: 'retain',
          eventTimeFloor: request.eventTimeFloor,
          removedEventLedgerRows: removedEvents.rowCount,
          removedBatchEventMappings: removedMappings.rowCount,
          publicationRevision: currentRevision,
          publicationsPreserved: true,
        },
        now,
      });
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async createQuery({ principal, requestKey, ...query }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `query:${principal.length}:${principal}:${requestKey.length}:${requestKey}`);
      const existing = await client.query(`
        SELECT intent_digest, result_json FROM aca_queries WHERE principal = $1 AND request_key = $2
      `, [principal, requestKey]);
      if (existing.rowCount) {
        if (existing.rows[0].intent_digest !== query.intentDigest) throw conflict();
        await client.query('COMMIT');
        return { ...existing.rows[0].result_json, replayed: true };
      }
      const state = await client.query(`SELECT * FROM aca_state WHERE singleton = true`);
      const selectedRevision = query.publicationRevision ?? (
        state.rows[0].latest_publication_revision === null ? 0 : integer(state.rows[0].latest_publication_revision)
      );
      if (selectedRevision === 0) throw notFound('publication was not found');
      const publicationResult = await client.query(`
        SELECT * FROM aca_publications WHERE publication_revision = $1
      `, [selectedRevision]);
      if (!publicationResult.rowCount) throw notFound('publication was not found');
      const publication = publicationResult.rows[0];
      const finalizedThrough = timestamp(publication.finalized_through);
      if (query.kind === 'top' && query.end > finalizedThrough) throw windowNotPublished();
      const rowsResult = await client.query(`
        SELECT window_start, window_end, ad_id, region, device, count
        FROM aca_publication_rows
        WHERE publication_revision = $1 AND window_start >= $2 AND window_start < $3
        ORDER BY window_start, ad_id, region, device
      `, [selectedRevision, query.start, query.end]);
      const rows = rowsResult.rows.map((row) => ({
        windowStart: timestamp(row.window_start),
        windowEnd: timestamp(row.window_end),
        adId: row.ad_id,
        region: row.region,
        device: row.device,
        count: integer(row.count),
      }));
      const correctionsResult = await client.query(`
        SELECT disposition.window_start, disposition.ad_id, disposition.region, disposition.device
        FROM aca_corrections AS correction
        JOIN aca_event_dispositions AS disposition
          ON disposition.definition_id = $2 AND disposition.event_id = correction.event_id
        WHERE correction.applied_publication_revision <= $1
          AND disposition.window_start >= $3 AND disposition.window_start < $4
      `, [selectedRevision, labContract.definition, query.start, query.end]);
      const corrections = correctionsResult.rows.map((row) => ({
        windowStart: timestamp(row.window_start),
        adId: row.ad_id,
        region: row.region,
        device: row.device,
      }));
      const resultRows = query.kind === 'count'
        ? materializeCountRows({
          rows,
          corrections,
          query,
          finalizedThrough,
          retentionFloor: timestamp(state.rows[0].retention_event_time_floor),
        })
        : materializeTopRows({ rows, query });
      const queryId = randomUUID();
      const firstPage = pageRows(resultRows, query.pageSize, 0);
      const result = {
        queryId,
        kind: query.kind,
        publicationRevision: selectedRevision,
        correctionGeneration: integer(publication.correction_generation),
        globalWatermark: timestamp(publication.global_watermark),
        finalizedThrough,
        watermarkKind: labContract.watermarkKind,
        finalizedByPolicy: true,
        completenessProved: false,
        measure: labContract.measure,
        fraudValidityProved: false,
        attributionProved: false,
        billingProved: false,
        chargeProved: false,
        externalAcceptanceProved: false,
        pageSize: query.pageSize,
        pageCount: firstPage.pageCount,
        rows: resultRows,
      };
      const now = await this.#databaseNow(client);
      await client.query(`
        INSERT INTO aca_queries (
          query_id, principal, request_key, intent_digest,
          publication_revision, result_json, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
      `, [queryId, principal, requestKey, query.intentDigest, selectedRevision, JSON.stringify(result), now]);
      await client.query('COMMIT');
      return { ...result, replayed: false };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async getQueryPage(queryId, page) {
    const found = await this.pool.query(`SELECT result_json FROM aca_queries WHERE query_id = $1`, [queryId]);
    if (!found.rowCount) throw notFound('query was not found');
    const result = found.rows[0].result_json;
    const selected = pageRows(result.rows, integer(result.pageSize), page);
    if (selected === null) throw notFound('query page was not found');
    return {
      queryId,
      publicationRevision: integer(result.publicationRevision),
      page: selected.page,
      pageCount: selected.pageCount,
      rows: selected.rows,
      replayed: true,
    };
  }
}
