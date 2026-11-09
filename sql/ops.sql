CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS cdc_offsets (
    pipeline_name text PRIMARY KEY, last_lsn text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ops.raw_events (
    event_id bigserial PRIMARY KEY, trace_id text NOT NULL UNIQUE, source_table text NOT NULL,
    action text NOT NULL, record_key text NOT NULL, orderid integer, payload jsonb NOT NULL,
    source_lsn text NOT NULL, status text NOT NULL, retry_count integer NOT NULL DEFAULT 0,
    received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz, error_message text,
    kafka_topic text, kafka_partition integer, kafka_offset bigint
);
CREATE TABLE IF NOT EXISTS ops.pipeline_events (
    id bigserial PRIMARY KEY, trace_id text NOT NULL, pipeline_name text NOT NULL,
    stage text NOT NULL, status text NOT NULL, message text, metadata jsonb, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS trace_stage ON ops.pipeline_events(trace_id,id DESC);
CREATE TABLE IF NOT EXISTS ops.alerts (
    alert_id bigserial PRIMARY KEY, trace_id text, pipeline_name text NOT NULL, stage text NOT NULL,
    severity text NOT NULL, message text NOT NULL, sms_status text NOT NULL, provider text,
    send_attempts integer NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now(),
    sms_sent_at timestamptz, provider_id bigint, dedup_key text UNIQUE
);
CREATE TABLE IF NOT EXISTS ops.job_stops (
    dag_id text NOT NULL, run_id text NOT NULL, requested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(dag_id,run_id)
);
CREATE TABLE IF NOT EXISTS ops.installation (step text PRIMARY KEY, finished_at timestamptz NOT NULL DEFAULT now());
CREATE OR REPLACE VIEW ops.pipeline_metrics AS
SELECT count(*) AS total_events, count(*) FILTER(WHERE status='processed') AS processed_events,
count(*) FILTER(WHERE status IN ('published','received','processing')) AS active_events,
count(*) FILTER(WHERE status='failed') AS failed_events,
count(*) FILTER(WHERE received_at>=now()-interval '24 hours') AS events_24h,
round(avg(extract(epoch FROM processed_at-received_at)*1000)::numeric,1)::float AS avg_latency_ms
FROM ops.raw_events;
CREATE OR REPLACE VIEW ops.event_trace AS
SELECT r.event_id,r.trace_id,r.source_table,r.action,r.orderid,(r.payload->>'productid')::integer AS productid,
r.status AS event_status,r.kafka_topic,r.kafka_partition,r.kafka_offset,r.received_at,r.processed_at,
r.retry_count,r.error_message,s.stage AS current_stage,s.status AS current_stage_status FROM ops.raw_events r
LEFT JOIN LATERAL (SELECT stage,status FROM ops.pipeline_events p WHERE p.trace_id=r.trace_id ORDER BY id DESC LIMIT 1) s ON true;
