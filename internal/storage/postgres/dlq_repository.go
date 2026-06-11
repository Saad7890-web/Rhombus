package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/google/uuid"
)

type DLQItem struct {
	EventID            string          `json:"event_id"`
	TenantID           *string         `json:"tenant_id,omitempty"`
	AggregateType      string          `json:"aggregate_type"`
	AggregateID        string          `json:"aggregate_id"`
	OrderingKey        string          `json:"ordering_key"`
	EventType          string          `json:"event_type"`
	SchemaVersion      int             `json:"schema_version"`
	Payload            json.RawMessage `json:"payload"`
	Metadata           json.RawMessage `json:"metadata"`
	Destination        json.RawMessage `json:"destination"`
	RetryCount         int             `json:"retry_count"`
	LastError          string          `json:"last_error"`
	StackTrace         *string         `json:"stack_trace,omitempty"`
	WorkerID           *string         `json:"worker_id,omitempty"`
	OriginalStatus     string          `json:"original_status"`
	CreatedAt          time.Time       `json:"created_at"`
	MovedToDLQAt       time.Time       `json:"moved_to_dlq_at"`
	ReplayedAt         *time.Time      `json:"replayed_at,omitempty"`
}

type ReplayRequest struct {
	ReplayedBy string `json:"replayed_by"`
	Notes      string `json:"notes"`
}

func (r *OutboxRepository) ListDLQ(ctx context.Context, limit, offset int) ([]DLQItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT
			event_id, tenant_id, aggregate_type, aggregate_id, ordering_key,
			event_type, schema_version, payload, metadata, destination,
			retry_count, last_error, stack_trace, worker_id, original_status,
			created_at, moved_to_dlq_at, replayed_at
		FROM rhombus_dlq
		ORDER BY moved_to_dlq_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.Pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []DLQItem
	for rows.Next() {
		var item DLQItem
		err := rows.Scan(
			&item.EventID, &item.TenantID, &item.AggregateType, &item.AggregateID, &item.OrderingKey,
			&item.EventType, &item.SchemaVersion, &item.Payload, &item.Metadata, &item.Destination,
			&item.RetryCount, &item.LastError, &item.StackTrace, &item.WorkerID, &item.OriginalStatus,
			&item.CreatedAt, &item.MovedToDLQAt, &item.ReplayedAt,
		)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	return items, rows.Err()
}

func (r *OutboxRepository) GetDLQ(ctx context.Context, eventID string) (*DLQItem, error) {
	query := `
		SELECT
			event_id, tenant_id, aggregate_type, aggregate_id, ordering_key,
			event_type, schema_version, payload, metadata, destination,
			retry_count, last_error, stack_trace, worker_id, original_status,
			created_at, moved_to_dlq_at, replayed_at
		FROM rhombus_dlq
		WHERE event_id = $1
	`

	var item DLQItem
	err := r.db.Pool.QueryRow(ctx, query, eventID).Scan(
		&item.EventID, &item.TenantID, &item.AggregateType, &item.AggregateID, &item.OrderingKey,
		&item.EventType, &item.SchemaVersion, &item.Payload, &item.Metadata, &item.Destination,
		&item.RetryCount, &item.LastError, &item.StackTrace, &item.WorkerID, &item.OriginalStatus,
		&item.CreatedAt, &item.MovedToDLQAt, &item.ReplayedAt,
	)
	if err != nil {
		return nil, err
	}

	return &item, nil
}

func (r *OutboxRepository) ReplayDLQ(ctx context.Context, eventID string, replayedBy string, notes string) (string, error) {
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	var item DLQItem
	row := tx.QueryRow(ctx, `
		SELECT
			event_id, tenant_id, aggregate_type, aggregate_id, ordering_key,
			event_type, schema_version, payload, metadata, destination,
			retry_count, last_error, stack_trace, worker_id, original_status,
			created_at, moved_to_dlq_at, replayed_at
		FROM rhombus_dlq
		WHERE event_id = $1
		FOR UPDATE
	`, eventID)

	err = row.Scan(
		&item.EventID, &item.TenantID, &item.AggregateType, &item.AggregateID, &item.OrderingKey,
		&item.EventType, &item.SchemaVersion, &item.Payload, &item.Metadata, &item.Destination,
		&item.RetryCount, &item.LastError, &item.StackTrace, &item.WorkerID, &item.OriginalStatus,
		&item.CreatedAt, &item.MovedToDLQAt, &item.ReplayedAt,
	)
	if err != nil {
		return "", err
	}

	if item.ReplayedAt != nil {
		return "", fmt.Errorf("dlq event %s already replayed", eventID)
	}

	newEventID := uuid.NewString()

	newMetadata, err := mergeReplayMetadata(item.Metadata, item.EventID, replayedBy, notes)
	if err != nil {
		return "", err
	}

	evt := &outbox.Event{
		ID:            newEventID,
		TenantID:      item.TenantID,
		AggregateType: item.AggregateType,
		AggregateID:   item.AggregateID,
		OrderingKey:   item.OrderingKey,
		EventType:     item.EventType,
		SchemaVersion: item.SchemaVersion,
		Payload:       append([]byte(nil), item.Payload...),
		Metadata:      newMetadata,
		Destination:   append([]byte(nil), item.Destination...),
		Status:        outbox.StatusPending,
		RetryCount:    0,
		LastError:     nil,
		AvailableAt:   time.Now().UTC(),
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}

	if err := r.InsertTx(ctx, tx, evt); err != nil {
		return "", err
	}

	_, err = tx.Exec(ctx, `
		UPDATE rhombus_dlq
		SET replayed_at = NOW()
		WHERE event_id = $1
	`, eventID)
	if err != nil {
		return "", err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO rhombus_replay_audit (
			dlq_event_id,
			new_outbox_event_id,
			replayed_by,
			replay_notes,
			original_destination,
			replayed_at
		) VALUES ($1, $2, $3, $4, $5, NOW())
	`, eventID, newEventID, nullableString(replayedBy), nullableString(notes), item.Destination)
	if err != nil {
		return "", err
	}

	if err = tx.Commit(ctx); err != nil {
		return "", err
	}

	return newEventID, nil
}

func mergeReplayMetadata(raw json.RawMessage, originalEventID string, replayedBy string, notes string) ([]byte, error) {
	var meta map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, err
		}
	}
	if meta == nil {
		meta = map[string]any{}
	}
	meta["replayed_from_dlq_event_id"] = originalEventID
	if replayedBy != "" {
		meta["replayed_by"] = replayedBy
	}
	if notes != "" {
		meta["replay_notes"] = notes
	}
	meta["replayed_at"] = time.Now().UTC().Format(time.RFC3339Nano)

	return json.Marshal(meta)
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

var _ = errors.New