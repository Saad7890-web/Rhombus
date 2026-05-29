package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/Saad7890-web/rhombus/internal/dispatcher"
	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
)

type testProcessor struct{}

func (p *testProcessor) Process(ctx context.Context, e outbox.Event) error {
	return nil
}

func TestWorker_ProcessesEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	_, _ = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)

	ev := &outbox.Event{
		AggregateType: "order",
		AggregateID:   "456",
		OrderingKey:   "order-456",
		EventType:     "orders.created",
		SchemaVersion:  1,
		Payload:       []byte(`{"order_id":"456"}`),
	}
	err = repo.Insert(ctx, ev)
	require.NoError(t, err)

	worker := dispatcher.NewWorker(
		"worker-test",
		repo,
		&testProcessor{},
		10,
		200*time.Millisecond,
		5*time.Second,
		3,
	)

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	done := make(chan error, 1)
	go func() {
		done <- worker.Run(runCtx)
	}()

	
	require.Eventually(t, func() bool {
		var status string
		err := pool.QueryRow(ctx, `SELECT status FROM rhombus_outbox WHERE id = $1`, ev.ID).Scan(&status)
		if err != nil {
			return false
		}
		return status == string(outbox.StatusDelivered)
	}, 5*time.Second, 200*time.Millisecond)

	runCancel()
	_ = <-done
}