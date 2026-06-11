CREATE TABLE IF NOT EXISTS rhombus_replay_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    dlq_event_id UUID NOT NULL,
    new_outbox_event_id UUID NOT NULL,

    replayed_by TEXT NULL,
    replay_notes TEXT NULL,

    original_destination JSONB NOT NULL DEFAULT '{}'::jsonb,
    replayed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rhombus_replay_audit_dlq_event_id
    ON rhombus_replay_audit (dlq_event_id);

CREATE INDEX IF NOT EXISTS idx_rhombus_replay_audit_replayed_at
    ON rhombus_replay_audit (replayed_at DESC);