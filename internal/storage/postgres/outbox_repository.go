package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type OutboxRepository struct {
	db *DB
}

func NewOutboxRepository(db *DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) Insert(ctx context.Context, e *outbox.Event) error {
	if e == nil {
		return errors.New("event is nil")
	}
	if e.ID == "" {
		e.ID = uuid.NewString()
	}
	if e.Status == "" {
		e.Status = outbox.StatusPending
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = 1
	}
	if e.AvailableAt.IsZero() {
		e.AvailableAt = time.Now().UTC()
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	e.UpdatedAt = time.Now().UTC()

	metadata := e.Metadata
	if len(metadata) == 0 {
		metadata = []byte(`{}`)
	}
	destination := e.Destination
	if len(destination) == 0 {
		destination = []byte(`{}`)
	}

	query := `
		INSERT INTO rhombus_outbox (
			id, tenant_id, aggregate_type, aggregate_id, ordering_key,
			event_type, schema_version, payload, metadata, destination,
			status, retry_count, last_error, available_at, leased_until,
			leased_by, trace_id, correlation_id, idempotency_key,
			created_at, updated_at, processed_at
		) VALUES (
			@id, @tenant_id, @aggregate_type, @aggregate_id, @ordering_key,
			@event_type, @schema_version, @payload, @metadata, @destination,
			@status, @retry_count, @last_error, @available_at, @leased_until,
			@leased_by, @trace_id, @correlation_id, @idempotency_key,
			@created_at, @updated_at, @processed_at
		)
	`

	_, err := r.db.Pool.Exec(ctx, query, pgx.NamedArgs{
		"id":              e.ID,
		"tenant_id":       e.TenantID,
		"aggregate_type":  e.AggregateType,
		"aggregate_id":    e.AggregateID,
		"ordering_key":     e.OrderingKey,
		"event_type":      e.EventType,
		"schema_version":  e.SchemaVersion,
		"payload":         e.Payload,
		"metadata":        metadata,
		"destination":     destination,
		"status":          e.Status,
		"retry_count":     e.RetryCount,
		"last_error":      e.LastError,
		"available_at":    e.AvailableAt,
		"leased_until":    e.LeasedUntil,
		"leased_by":       e.LeasedBy,
		"trace_id":        e.TraceID,
		"correlation_id":  e.CorrelationID,
		"idempotency_key": e.IdempotencyKey,
		"created_at":      e.CreatedAt,
		"updated_at":      e.UpdatedAt,
		"processed_at":    e.ProcessedAt,
	})
	return err
}

func (r *OutboxRepository) FetchEligible(ctx context.Context, limit int, now time.Time) ([]outbox.Event, error) {
	query := `
		SELECT
			id, tenant_id, aggregate_type, aggregate_id, ordering_key,
			event_type, schema_version, payload, metadata, destination,
			status, retry_count, last_error, available_at, leased_until,
			leased_by, trace_id, correlation_id, idempotency_key,
			created_at, updated_at, processed_at
		FROM rhombus_outbox
		WHERE status IN ('PENDING', 'RETRY_WAIT')
		  AND available_at <= $1
		ORDER BY created_at ASC
		LIMIT $2
	`

	rows, err := r.db.Pool.Query(ctx, query, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []outbox.Event
	for rows.Next() {
		var e outbox.Event
		var tenantID, lastError, leasedBy, traceID, correlationID, idempotencyKey *string
		var leasedUntil, processedAt *time.Time
		var metadata, destination []byte

		err := rows.Scan(
			&e.ID, &tenantID, &e.AggregateType, &e.AggregateID, &e.OrderingKey,
			&e.EventType, &e.SchemaVersion, &e.Payload, &metadata, &destination,
			&e.Status, &e.RetryCount, &lastError, &e.AvailableAt, &leasedUntil,
			&leasedBy, &traceID, &correlationID, &idempotencyKey,
			&e.CreatedAt, &e.UpdatedAt, &processedAt,
		)
		if err != nil {
			return nil, err
		}

		e.TenantID = tenantID
		e.Metadata = metadata
		e.Destination = destination
		e.LastError = lastError
		e.LeasedUntil = leasedUntil
		e.LeasedBy = leasedBy
		e.TraceID = traceID
		e.CorrelationID = correlationID
		e.IdempotencyKey = idempotencyKey
		e.ProcessedAt = processedAt

		items = append(items, e)
	}

	return items, rows.Err()
}

func (r *OutboxRepository) Lease(ctx context.Context, id string, workerID string, leaseUntil time.Time) (bool, error) {
	query := `
		UPDATE rhombus_outbox
		SET status = 'PROCESSING',
			leased_by = $2,
			leased_until = $3,
			updated_at = NOW()
		WHERE id = $1
		  AND status IN ('PENDING', 'RETRY_WAIT')
		  AND (leased_until IS NULL OR leased_until < NOW())
	`
	res, err := r.db.Pool.Exec(ctx, query, id, workerID, leaseUntil)
	if err != nil {
		return false, err
	}
	return res.RowsAffected() == 1, nil
}

func (r *OutboxRepository) MarkDelivered(ctx context.Context, id string) error {
	query := `
		UPDATE rhombus_outbox
		SET status = 'DELIVERED',
			processed_at = NOW(),
			leased_until = NULL,
			leased_by = NULL,
			last_error = NULL,
			updated_at = NOW()
		WHERE id = $1
	`
	_, err := r.db.Pool.Exec(ctx, query, id)
	return err
}

func (r *OutboxRepository) MarkRetryWait(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error {
	query := `
		UPDATE rhombus_outbox
		SET status = 'RETRY_WAIT',
			retry_count = $2,
			available_at = $3,
			last_error = $4,
			leased_until = NULL,
			leased_by = NULL,
			updated_at = NOW()
		WHERE id = $1
	`
	_, err := r.db.Pool.Exec(ctx, query, id, retryCount, availableAt, lastError)
	return err
}

func (r *OutboxRepository) MoveToDLQ(ctx context.Context, id string, lastError string) error {
	query := `
		UPDATE rhombus_outbox
		SET status = 'DLQ',
			last_error = $2,
			leased_until = NULL,
			leased_by = NULL,
			updated_at = NOW()
		WHERE id = $1
	`
	_, err := r.db.Pool.Exec(ctx, query, id, lastError)
	return err
}

func DecodeMetadata(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *OutboxRepository) ClaimBatch(
	ctx context.Context,
	workerID string,
	limit int,
	leaseDuration time.Duration,
	now time.Time,
) ([]outbox.Event, error) {
	leaseUntil := now.Add(leaseDuration)

	query := `
		WITH claimed AS (
			SELECT id
			FROM rhombus_outbox
			WHERE status IN ('PENDING', 'RETRY_WAIT')
			  AND available_at <= $1
			  AND (leased_until IS NULL OR leased_until < $1)
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE rhombus_outbox o
		SET status = 'PROCESSING',
			leased_by = $3,
			leased_until = $4,
			updated_at = NOW()
		FROM claimed
		WHERE o.id = claimed.id
		RETURNING
			o.id, o.tenant_id, o.aggregate_type, o.aggregate_id, o.ordering_key,
			o.event_type, o.schema_version, o.payload, o.metadata, o.destination,
			o.status, o.retry_count, o.last_error, o.available_at, o.leased_until,
			o.leased_by, o.trace_id, o.correlation_id, o.idempotency_key,
			o.created_at, o.updated_at, o.processed_at
	`

	rows, err := r.db.Pool.Query(ctx, query, now, limit, workerID, leaseUntil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []outbox.Event
	for rows.Next() {
		var e outbox.Event
		var tenantID, lastError, leasedBy, traceID, correlationID, idempotencyKey *string
		var leasedUntil, processedAt *time.Time
		var metadata, destination []byte

		if err := rows.Scan(
			&e.ID, &tenantID, &e.AggregateType, &e.AggregateID, &e.OrderingKey,
			&e.EventType, &e.SchemaVersion, &e.Payload, &metadata, &destination,
			&e.Status, &e.RetryCount, &lastError, &e.AvailableAt, &leasedUntil,
			&leasedBy, &traceID, &correlationID, &idempotencyKey,
			&e.CreatedAt, &e.UpdatedAt, &processedAt,
		); err != nil {
			return nil, err
		}

		e.TenantID = tenantID
		e.Metadata = metadata
		e.Destination = destination
		e.LastError = lastError
		e.LeasedUntil = leasedUntil
		e.LeasedBy = leasedBy
		e.TraceID = traceID
		e.CorrelationID = correlationID
		e.IdempotencyKey = idempotencyKey
		e.ProcessedAt = processedAt

		items = append(items, e)
	}

	return items, rows.Err()
}

func (r *OutboxRepository) ResetStaleLeases(ctx context.Context, before time.Time) error {
	query := `
		UPDATE rhombus_outbox
		SET status = 'RETRY_WAIT',
			leased_until = NULL,
			leased_by = NULL,
			available_at = NOW(),
			updated_at = NOW()
		WHERE status = 'PROCESSING'
		  AND leased_until IS NOT NULL
		  AND leased_until < $1
	`
	_, err := r.db.Pool.Exec(ctx, query, before)
	return err
}