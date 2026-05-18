CREATE TABLE IF NOT EXISTS rhombus_outbox (
    id UUID PRIMARY KEY,

    tenant_id TEXT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    ordering_key TEXT NOT NULL,

    event_type TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,

    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    destination JSONB NOT NULL DEFAULT '{}'::jsonb,

    status TEXT NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,

    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    leased_until TIMESTAMPTZ NULL,
    leased_by TEXT NULL,

    trace_id TEXT NULL,
    correlation_id TEXT NULL,
    idempotency_key TEXT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,

    CONSTRAINT rhombus_outbox_status_chk
        CHECK (status IN ('PENDING', 'PROCESSING', 'DELIVERED', 'RETRY_WAIT', 'FAILED', 'DLQ'))
);

CREATE INDEX IF NOT EXISTS idx_rhombus_outbox_pending
    ON rhombus_outbox (status, available_at, created_at);

CREATE INDEX IF NOT EXISTS idx_rhombus_outbox_ordering
    ON rhombus_outbox (ordering_key, created_at);

CREATE INDEX IF NOT EXISTS idx_rhombus_outbox_lease
    ON rhombus_outbox (leased_until)
    WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS idx_rhombus_outbox_aggregate
    ON rhombus_outbox (aggregate_type, aggregate_id, created_at);