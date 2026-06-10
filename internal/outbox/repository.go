package outbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type Repository interface {
	Insert(ctx context.Context, e *Event) error
	InsertTx(ctx context.Context, tx pgx.Tx, e *Event) error
	FetchEligible(ctx context.Context, limit int, now time.Time) ([]Event, error)
	Lease(ctx context.Context, id string, workerID string, leaseUntil time.Time) (bool, error)
	MarkDelivered(ctx context.Context, id string) error
	MarkRetryWait(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error
	MoveToDLQ(ctx context.Context, id string, lastError string) error
	ResetStaleLeases(ctx context.Context, before time.Time) error
}