CREATE TABLE IF NOT EXISTS aca_state (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  ingest_revision bigint NOT NULL CHECK (ingest_revision >= 0),
  next_publication_revision bigint NOT NULL CHECK (next_publication_revision >= 1),
  latest_publication_revision bigint,
  correction_generation bigint NOT NULL CHECK (correction_generation >= 0),
  retention_event_time_floor timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

INSERT INTO aca_state (
  singleton, ingest_revision, next_publication_revision, latest_publication_revision,
  correction_generation, retention_event_time_floor, updated_at
) VALUES (
  true, 0, 1, NULL, 0, '2020-01-01T00:00:00.000Z', clock_timestamp()
) ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS aca_sources (
  source_id text PRIMARY KEY CHECK (source_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
  partition_id integer NOT NULL UNIQUE CHECK (partition_id IN (0, 1)),
  source_epoch bigint NOT NULL CHECK (source_epoch >= 0),
  next_sequence bigint NOT NULL CHECK (next_sequence >= 0),
  next_batch_revision bigint NOT NULL CHECK (next_batch_revision >= 1),
  next_input_offset bigint NOT NULL CHECK (next_input_offset >= 0),
  latest_source_watermark timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS aca_batches (
  batch_id uuid PRIMARY KEY,
  source_id text NOT NULL REFERENCES aca_sources(source_id),
  source_epoch bigint NOT NULL CHECK (source_epoch >= 0),
  batch_sequence bigint NOT NULL CHECK (batch_sequence >= 0),
  batch_revision bigint NOT NULL CHECK (batch_revision >= 1),
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  source_watermark timestamptz NOT NULL,
  input_start_offset bigint NOT NULL CHECK (input_start_offset >= 0),
  input_end_offset bigint NOT NULL CHECK (input_end_offset >= input_start_offset),
  submitted_count integer NOT NULL CHECK (submitted_count BETWEEN 0 AND 64),
  new_count integer NOT NULL CHECK (new_count BETWEEN 0 AND 64),
  duplicate_count integer NOT NULL CHECK (duplicate_count BETWEEN 0 AND 64),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  UNIQUE (source_id, source_epoch, batch_sequence),
  UNIQUE (source_id, batch_revision),
  CHECK (new_count + duplicate_count = submitted_count),
  CHECK (input_end_offset = input_start_offset + new_count)
);

CREATE TABLE IF NOT EXISTS aca_events (
  event_id text PRIMARY KEY CHECK (event_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$'),
  event_digest text NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
  owner_batch_id uuid NOT NULL,
  source_id text NOT NULL,
  partition_id integer NOT NULL CHECK (partition_id IN (0, 1)),
  input_offset bigint NOT NULL CHECK (input_offset >= 0),
  event_time timestamptz NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  ad_id text NOT NULL,
  region text NOT NULL,
  device text NOT NULL CHECK (device IN ('desktop', 'mobile', 'tablet')),
  ingest_revision bigint NOT NULL CHECK (ingest_revision >= 1),
  created_at timestamptz NOT NULL,
  UNIQUE (partition_id, input_offset),
  CHECK (window_end = window_start + interval '1 minute'),
  CHECK (event_time >= window_start AND event_time < window_end)
);

CREATE TABLE IF NOT EXISTS aca_batch_events (
  batch_id uuid NOT NULL REFERENCES aca_batches(batch_id) ON DELETE CASCADE,
  ordinal integer NOT NULL CHECK (ordinal BETWEEN 0 AND 63),
  event_id text NOT NULL,
  owned boolean NOT NULL,
  PRIMARY KEY (batch_id, ordinal),
  UNIQUE (batch_id, event_id)
);

CREATE TABLE IF NOT EXISTS aca_assignments (
  partition_id integer PRIMARY KEY CHECK (partition_id IN (0, 1)),
  current_generation bigint NOT NULL CHECK (current_generation >= 0),
  current_worker_id text,
  checkpoint_batch_revision bigint NOT NULL CHECK (checkpoint_batch_revision >= 0),
  processed_watermark timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CHECK ((current_generation = 0 AND current_worker_id IS NULL) OR (current_generation > 0 AND current_worker_id IS NOT NULL))
);

INSERT INTO aca_assignments (
  partition_id, current_generation, current_worker_id,
  checkpoint_batch_revision, processed_watermark, updated_at
) VALUES
  (0, 0, NULL, 0, '2020-01-01T00:00:00.000Z', clock_timestamp()),
  (1, 0, NULL, 0, '2020-01-01T00:00:00.000Z', clock_timestamp())
ON CONFLICT (partition_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS aca_assignment_history (
  partition_id integer NOT NULL,
  generation bigint NOT NULL CHECK (generation >= 1),
  worker_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('current', 'revoked')),
  assigned_at timestamptz NOT NULL,
  revoked_at timestamptz,
  PRIMARY KEY (partition_id, generation),
  CHECK ((status = 'current' AND revoked_at IS NULL) OR (status = 'revoked' AND revoked_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS aca_one_current_assignment
ON aca_assignment_history(partition_id) WHERE status = 'current';

CREATE TABLE IF NOT EXISTS aca_mutation_receipts (
  principal text NOT NULL,
  request_key text NOT NULL,
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  operation text NOT NULL CHECK (operation IN ('rotate_epoch', 'assign', 'publish', 'retain')),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (principal, request_key)
);

CREATE TABLE IF NOT EXISTS aca_process_receipts (
  partition_id integer NOT NULL,
  generation bigint NOT NULL,
  request_key text NOT NULL,
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  batch_revision bigint NOT NULL CHECK (batch_revision >= 1),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (partition_id, generation, request_key)
);

CREATE TABLE IF NOT EXISTS aca_event_dispositions (
  definition_id text NOT NULL,
  event_id text NOT NULL,
  batch_id uuid NOT NULL,
  partition_id integer NOT NULL CHECK (partition_id IN (0, 1)),
  batch_revision bigint NOT NULL CHECK (batch_revision >= 1),
  assignment_generation bigint NOT NULL CHECK (assignment_generation >= 1),
  event_time timestamptz NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  ad_id text NOT NULL,
  region text NOT NULL,
  device text NOT NULL,
  disposition text NOT NULL CHECK (disposition IN ('counted', 'late_unapplied')),
  created_at timestamptz NOT NULL,
  PRIMARY KEY (definition_id, event_id)
);

CREATE TABLE IF NOT EXISTS aca_window_partials (
  definition_id text NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  ad_id text NOT NULL,
  region text NOT NULL,
  device text NOT NULL,
  count bigint NOT NULL CHECK (count > 0),
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (definition_id, window_start, ad_id, region, device),
  CHECK (window_end = window_start + interval '1 minute')
);

CREATE TABLE IF NOT EXISTS aca_publications (
  publication_revision bigint PRIMARY KEY CHECK (publication_revision >= 1),
  kind text NOT NULL CHECK (kind IN ('finalization', 'correction')),
  global_watermark timestamptz NOT NULL,
  finalized_through timestamptz NOT NULL,
  correction_generation bigint NOT NULL CHECK (correction_generation >= 0),
  retention_event_time_floor timestamptz NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS aca_publication_rows (
  publication_revision bigint NOT NULL REFERENCES aca_publications(publication_revision) ON DELETE CASCADE,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  ad_id text NOT NULL,
  region text NOT NULL,
  device text NOT NULL,
  count bigint NOT NULL CHECK (count > 0),
  PRIMARY KEY (publication_revision, window_start, ad_id, region, device)
);

CREATE TABLE IF NOT EXISTS aca_corrections (
  correction_id text PRIMARY KEY,
  principal text NOT NULL,
  request_key text NOT NULL,
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  event_id text NOT NULL UNIQUE,
  delta integer NOT NULL CHECK (delta = 1),
  correction_generation bigint NOT NULL UNIQUE CHECK (correction_generation >= 1),
  applied_publication_revision bigint NOT NULL UNIQUE REFERENCES aca_publications(publication_revision),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  UNIQUE (principal, request_key)
);

CREATE TABLE IF NOT EXISTS aca_queries (
  query_id uuid PRIMARY KEY,
  principal text NOT NULL,
  request_key text NOT NULL,
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  publication_revision bigint NOT NULL REFERENCES aca_publications(publication_revision),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  UNIQUE (principal, request_key)
);

CREATE INDEX IF NOT EXISTS aca_events_owner_batch ON aca_events(owner_batch_id);
CREATE INDEX IF NOT EXISTS aca_events_time ON aca_events(event_time);
CREATE INDEX IF NOT EXISTS aca_dispositions_window ON aca_event_dispositions(window_start, disposition);
CREATE INDEX IF NOT EXISTS aca_publication_rows_query ON aca_publication_rows(publication_revision, window_start, ad_id);
