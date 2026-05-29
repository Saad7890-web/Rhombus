package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
)

func TestOutboxRepository_InsertAndClaimBatch(t *testing.T) {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)
	}
	cleanup()
	defer cleanup()

	ev := &outbox.Event{
		AggregateType: "order",
		AggregateID:   "123",
		OrderingKey:   "order-123",
		EventType:     "orders.created",
		SchemaVersion:  1,
		Payload:       []byte(`{"order_id":"123"}`),
		Metadata:      []byte(`{"source":"test"}`),
		Destination:   []byte(`{"kafka":{"topic":"orders.created"}}`),
	}

	err = repo.Insert(ctx, ev)
	require.NoError(t, err)
	require.NotEmpty(t, ev.ID)

	events, err := repo.ClaimBatch(ctx, "worker-1", 10, 30*time.Second, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, ev.ID, events[0].ID)
	require.Equal(t, outbox.StatusProcessing, events[0].Status)

	err = repo.MarkDelivered(ctx, ev.ID)
	require.NoError(t, err)

	var status string
	err = pool.QueryRow(ctx, `SELECT status FROM rhombus_outbox WHERE id = $1`, ev.ID).Scan(&status)
	require.NoError(t, err)
	require.Equal(t, string(outbox.StatusDelivered), status)
}