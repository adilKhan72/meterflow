-- Applied idempotently by meterflow.load.apply_schema (ADR-0004). Runs once per DAG run (ensure_schema task),
-- serialised by an advisory lock, and takes no heavy locks when everything already exists.
SELECT pg_advisory_xact_lock(hashtext('meterflow.schema'));
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS readings (
    meter_id      text             NOT NULL,
    ts            timestamptz      NOT NULL,
    kwh           double precision NOT NULL,
    voltage       double precision NOT NULL,
    delivered_at  timestamptz      NOT NULL,
    estimated     boolean          NOT NULL DEFAULT false,
    spike         boolean          NOT NULL DEFAULT false,
    outage        boolean          NOT NULL DEFAULT false,
    batch_id      text             NOT NULL,
    PRIMARY KEY (meter_id, ts)
);
SELECT create_hypertable('readings', by_range('ts', INTERVAL '1 day'), if_not_exists => TRUE);

-- ALTER TABLE needs an exclusive lock: only run it the first time.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'readings' AND compression_enabled
    ) THEN
        ALTER TABLE readings SET (timescaledb.compress, timescaledb.compress_segmentby = 'meter_id', timescaledb.compress_orderby = 'ts DESC');
    END IF;
END $$;
SELECT add_compression_policy('readings', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('readings', INTERVAL '400 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS rejects (
    id           bigserial PRIMARY KEY,
    batch_id     text        NOT NULL,
    meter_id     text,
    ts           text,
    raw          jsonb       NOT NULL,
    reason       text        NOT NULL,
    rejected_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rejects_batch_idx ON rejects (batch_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS readings_hourly
WITH (timescaledb.continuous) AS
SELECT meter_id,
       time_bucket('1 hour', ts)              AS bucket,
       sum(kwh)                               AS kwh,
       count(*)                               AS readings,
       count(*) FILTER (WHERE estimated)      AS estimated_readings,
       bool_or(spike)                         AS any_spike,
       bool_or(outage)                        AS any_outage
FROM readings
GROUP BY meter_id, bucket
WITH NO DATA;
SELECT add_continuous_aggregate_policy('readings_hourly',
    start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE);

-- Hierarchical: daily totals are built from the hourly aggregate, not from raw readings.
CREATE MATERIALIZED VIEW IF NOT EXISTS readings_daily
WITH (timescaledb.continuous) AS
SELECT meter_id,
       time_bucket('1 day', bucket)           AS day,
       sum(kwh)                               AS kwh,
       sum(readings)                          AS readings,
       sum(estimated_readings)                AS estimated_readings
FROM readings_hourly
GROUP BY meter_id, day
WITH NO DATA;
SELECT add_continuous_aggregate_policy('readings_daily',
    start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- One row per pipeline run (ADR-0003): what came in, what was kept, and whether the quality gate passed.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              bigserial PRIMARY KEY,
    batch_id        text        NOT NULL,
    interval_start  timestamptz NOT NULL,
    raw_rows        integer     NOT NULL,
    clean_rows      integer     NOT NULL,
    rejected_rows   integer     NOT NULL,
    estimated_rows  integer     NOT NULL,
    spikes          integer     NOT NULL,
    expected_rows   integer     NOT NULL,
    stored_rows     integer     NOT NULL,
    reject_rate     double precision NOT NULL,
    coverage        double precision NOT NULL,
    freshness_lag_s double precision,
    passed          boolean     NOT NULL,
    failures        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pipeline_runs_batch_idx ON pipeline_runs (batch_id, recorded_at DESC);
