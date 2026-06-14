package tests

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
)

func TestOutboxRepository_ClaimBatchIsOrderedAndNonDuplicating(t *testing.T) {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	_, err = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)
	require.NoError(t, err)

	olderID := "11111111-1111-1111-1111-111111111111"
	newerID := "22222222-2222-2222-2222-222222222222"

	_, err = pool.Exec(ctx, `
		INSERT INTO rhombus_outbox (
			id, aggregate_type, aggregate_id, ordering_key, event_type, schema_version,
			payload, metadata, destination, status, retry_count, available_at,
			created_at, updated_at
		) VALUES
		($1, 'order', 'order-old', 'order-old', 'orders.created', 1, '{"order_id":"old"}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'PENDING', 0, NOW(), NOW() - INTERVAL '2 minutes', NOW() - INTERVAL '2 minutes'),
		($2, 'order', 'order-new', 'order-new', 'orders.created', 1, '{"order_id":"new"}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'PENDING', 0, NOW(), NOW() - INTERVAL '1 minute', NOW() - INTERVAL '1 minute')
	`, olderID, newerID)
	require.NoError(t, err)

	events, err := repo.ClaimBatch(ctx, "worker-1", 10, 30*time.Second, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, olderID, events[0].ID)
	require.Equal(t, newerID, events[1].ID)
	require.Equal(t, outbox.StatusProcessing, events[0].Status)
	require.Equal(t, outbox.StatusProcessing, events[1].Status)
}

func TestOutboxRepository_ConcurrentClaimBatchDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	_, err = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)
	require.NoError(t, err)

	eventID := "33333333-3333-3333-3333-333333333333"
	_, err = pool.Exec(ctx, `
		INSERT INTO rhombus_outbox (
			id, aggregate_type, aggregate_id, ordering_key, event_type, schema_version,
			payload, metadata, destination, status, retry_count, available_at,
			created_at, updated_at
		) VALUES
		($1, 'order', 'order-single', 'order-single', 'orders.created', 1, '{"order_id":"single"}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'PENDING', 0, NOW(), NOW(), NOW())
	`, eventID)
	require.NoError(t, err)

	var wg sync.WaitGroup
	results := make(chan []outbox.Event, 2)
	errorsCh := make(chan error, 2)

	claim := func(workerID string) {
		defer wg.Done()
		events, err := repo.ClaimBatch(ctx, workerID, 1, 30*time.Second, time.Now().UTC())
		if err != nil {
			errorsCh <- err
			return
		}
		results <- events
	}

	wg.Add(2)
	go claim("worker-a")
	go claim("worker-b")
	wg.Wait()
	close(results)
	close(errorsCh)

	for err := range errorsCh {
		require.NoError(t, err)
	}

	var totalClaimed int
	claimedIDs := map[string]struct{}{}
	for batch := range results {
		totalClaimed += len(batch)
		for _, e := range batch {
			claimedIDs[e.ID] = struct{}{}
		}
	}

	require.Equal(t, 1, totalClaimed)
	require.Len(t, claimedIDs, 1)
	require.Contains(t, claimedIDs, eventID)
}

func TestOutboxRepository_ResetStaleLeases_ReleasesProcessingEvent(t *testing.T) {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	_, err = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)
	require.NoError(t, err)

	eventID := "44444444-4444-4444-4444-444444444444"
	_, err = pool.Exec(ctx, `
		INSERT INTO rhombus_outbox (
			id, aggregate_type, aggregate_id, ordering_key, event_type, schema_version,
			payload, metadata, destination, status, retry_count, available_at,
			leased_until, leased_by, created_at, updated_at
		) VALUES
		($1, 'order', 'order-stale', 'order-stale', 'orders.created', 1,
		 '{"order_id":"stale"}'::jsonb, '{}'::jsonb, '{}'::jsonb,
		 'PROCESSING', 1, NOW() - INTERVAL '10 minutes',
		 NOW() - INTERVAL '5 minutes', 'dead-worker', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')
	`, eventID)
	require.NoError(t, err)

	err = repo.ResetStaleLeases(ctx, time.Now().UTC())
	require.NoError(t, err)

	var status string
	var leasedBy *string
	var leasedUntil *time.Time
	err = pool.QueryRow(ctx, `
		SELECT status, leased_by, leased_until
		FROM rhombus_outbox
		WHERE id = $1
	`, eventID).Scan(&status, &leasedBy, &leasedUntil)
	require.NoError(t, err)
	require.Equal(t, string(outbox.StatusRetryWait), status)
	require.Nil(t, leasedBy)
	require.Nil(t, leasedUntil)

	events, err := repo.ClaimBatch(ctx, "worker-recovery", 1, 30*time.Second, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, eventID, events[0].ID)
	require.Equal(t, outbox.StatusProcessing, events[0].Status)
}