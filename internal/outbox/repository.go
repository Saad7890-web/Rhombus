package outbox

import (
	"context"
	"time"
)

type Repository interface {
	Insert(ctx context.Context, e *Event) error
	FetchEligible(ctx context.Context, limit int, now time.Time) ([]Event, error)
	Lease(ctx context.Context, id string, workerID string, leaseUntil time.Time) (bool, error)
	MarkDelivered(ctx context.Context, id string) error
	MarkRetryWait(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error
	MoveToDLQ(ctx context.Context, id string, lastError string) error
}