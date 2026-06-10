package rhombus_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Saad7890-web/rhombus/pkg/rhombus"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestClient_TransactionCommitEnqueuesEvent(t *testing.T) {
	ctx := context.Background()
	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	client, err := rhombus.New(pool)
	require.NoError(t, err)

	_, _ = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)

	tx, err := client.BeginTransaction(ctx)
	require.NoError(t, err)

	event := &rhombus.Event{
		AggregateType: "order",
		AggregateID:   "tx-123",
		OrderingKey:   "order-tx-123",
		EventType:     "orders.created",
		SchemaVersion: 1,
		Payload:       []byte(`{"order_id":"tx-123"}`),
		AvailableAt:   time.Now().UTC(),
	}

	err = tx.EnqueueEvent(event)
	require.NoError(t, err)
	require.NotEmpty(t, event.ID)

	err = tx.Commit()
	require.NoError(t, err)

	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM rhombus_outbox WHERE id = $1`, event.ID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestClient_TransactionRollbackDoesNotPersistEvent(t *testing.T) {
	ctx := context.Background()
	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	client, err := rhombus.New(pool)
	require.NoError(t, err)

	_, _ = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)

	tx, err := client.BeginTransaction(ctx)
	require.NoError(t, err)

	event := &rhombus.Event{
		AggregateType: "order",
		AggregateID:   "tx-rollback-123",
		OrderingKey:   "order-tx-rollback-123",
		EventType:     "orders.created",
		SchemaVersion: 1,
		Payload:       []byte(`{"order_id":"tx-rollback-123"}`),
	}

	err = tx.EnqueueEvent(event)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM rhombus_outbox WHERE aggregate_id = $1`, event.AggregateID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}