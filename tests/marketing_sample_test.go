package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/Saad7890-web/rhombus/internal/dispatcher"
	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/Saad7890-web/rhombus/internal/retry"
	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
)

type marketingDemoProcessor struct{}

func (marketingDemoProcessor) Process(ctx context.Context, e outbox.Event) error {
	// Mark this as a non-retryable failure so the worker sends it straight to DLQ.
	return retry.NonRetryable(errors.New("marketing demo: forced failure for dashboard visibility"))
}

func TestMarketing_DemoDLQEventVisibleInDashboard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	aggregateID := fmt.Sprintf("marketing-demo-%d", time.Now().UnixNano())

	_, err = pool.Exec(ctx, `DELETE FROM rhombus_outbox WHERE aggregate_id = $1`, aggregateID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `DELETE FROM rhombus_dlq WHERE aggregate_id = $1`, aggregateID)
	require.NoError(t, err)

	ev := &outbox.Event{
		AggregateType: "order",
		AggregateID:   aggregateID,
		OrderingKey:   aggregateID,
		EventType:     "orders.created",
		SchemaVersion:  1,
		Payload: []byte(fmt.Sprintf(
			`{"order_id":"%s","customer_id":"customer-demo","amount_cents":1999,"demo":true}`,
			aggregateID,
		)),
		Metadata:    []byte(`{"source":"marketing-sample","campaign":"dashboard-demo"}`),
		Destination: []byte(`{"kafka":{"topic":"orders.created"},"demo":"dlq-dashboard"}`),
	}

	err = repo.Insert(ctx, ev)
	require.NoError(t, err)
	require.NotEmpty(t, ev.ID)

	worker := dispatcher.NewWorker(
		"marketing-demo-worker",
		repo,
		marketingDemoProcessor{},
		1,
		100*time.Millisecond,
		5*time.Second,
		3,
	)

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)

	go func() {
		done <- worker.Run(runCtx)
	}()

	require.Eventually(t, func() bool {
		var outboxStatus string
		var dlqCount int

		err1 := pool.QueryRow(ctx,
			`SELECT status FROM rhombus_outbox WHERE id = $1`,
			ev.ID,
		).Scan(&outboxStatus)
		err2 := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM rhombus_dlq WHERE event_id = $1`,
			ev.ID,
		).Scan(&dlqCount)

		return err1 == nil && err2 == nil && outboxStatus == string(outbox.StatusDLQ) && dlqCount == 1
	}, 8*time.Second, 100*time.Millisecond)

	runCancel()
	_ = <-done

	t.Logf("dashboard demo ready: event_id=%s aggregate_id=%s", ev.ID, aggregateID)
}