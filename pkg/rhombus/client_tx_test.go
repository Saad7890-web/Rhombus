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

func setupSampleSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS orders (
			id TEXT PRIMARY KEY,
			customer_id TEXT NOT NULL,
			amount_cents BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `DELETE FROM orders`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)
	require.NoError(t, err)
}

func TestWithTransaction_CommitsBusinessRowAndOutbox(t *testing.T) {
	ctx := context.Background()
	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	setupSampleSchema(t, pool)

	client, err := rhombus.New(pool)
	require.NoError(t, err)

	err = client.WithTransaction(ctx, func(tx *rhombus.Transaction) error {
		_, err := tx.Exec(
			`INSERT INTO orders (id, customer_id, amount_cents) VALUES ($1, $2, $3)`,
			"order-1001",
			"customer-77",
			int64(2500),
		)
		if err != nil {
			return err
		}

		return tx.EnqueueEvent(&rhombus.Event{
			AggregateType: "order",
			AggregateID:   "order-1001",
			OrderingKey:   "order-1001",
			EventType:     "orders.created",
			SchemaVersion: 1,
			Payload:       []byte(`{"order_id":"order-1001","customer_id":"customer-77","amount_cents":2500}`),
			Destination:   []byte(`{"kafka":{"topic":"orders.created"}}`),
			AvailableAt:   time.Now().UTC(),
		})
	})
	require.NoError(t, err)

	var orderCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM orders WHERE id = $1`, "order-1001").Scan(&orderCount)
	require.NoError(t, err)
	require.Equal(t, 1, orderCount)

	var outboxCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM rhombus_outbox WHERE aggregate_id = $1`, "order-1001").Scan(&outboxCount)
	require.NoError(t, err)
	require.Equal(t, 1, outboxCount)
}

func TestWithTransaction_RollsBackOnError(t *testing.T) {
	ctx := context.Background()
	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	setupSampleSchema(t, pool)

	client, err := rhombus.New(pool)
	require.NoError(t, err)

	err = client.WithTransaction(ctx, func(tx *rhombus.Transaction) error {
		_, err := tx.Exec(
			`INSERT INTO orders (id, customer_id, amount_cents) VALUES ($1, $2, $3)`,
			"order-rollback",
			"customer-88",
			int64(999),
		)
		if err != nil {
			return err
		}

		if err := tx.EnqueueEvent(&rhombus.Event{
			AggregateType: "order",
			AggregateID:   "order-rollback",
			OrderingKey:   "order-rollback",
			EventType:     "orders.created",
			SchemaVersion: 1,
			Payload:       []byte(`{"order_id":"order-rollback"}`),
			AvailableAt:   time.Now().UTC(),
		}); err != nil {
			return err
		}

		return context.Canceled
	})
	require.Error(t, err)

	var orderCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM orders WHERE id = $1`, "order-rollback").Scan(&orderCount)
	require.NoError(t, err)
	require.Equal(t, 0, orderCount)

	var outboxCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM rhombus_outbox WHERE aggregate_id = $1`, "order-rollback").Scan(&outboxCount)
	require.NoError(t, err)
	require.Equal(t, 0, outboxCount)
}