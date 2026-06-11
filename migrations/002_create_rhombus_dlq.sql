CREATE TABLE IF NOT EXISTS rhombus_dlq (
    event_id UUID PRIMARY KEY,

    tenant_id TEXT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    ordering_key TEXT NOT NULL,

    event_type TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,

    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    destination JSONB NOT NULL DEFAULT '{}'::jsonb,

    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL,
    stack_trace TEXT NULL,
    worker_id TEXT NULL,

    original_status TEXT NOT NULL DEFAULT 'DLQ',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    moved_to_dlq_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_rhombus_dlq_aggregate
    ON rhombus_dlq (aggregate_type, aggregate_id, moved_to_dlq_at DESC);

CREATE INDEX IF NOT EXISTS idx_rhombus_dlq_event_type
    ON rhombus_dlq (event_type, moved_to_dlq_at DESC);