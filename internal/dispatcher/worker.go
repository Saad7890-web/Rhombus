package dispatcher

import (
	"context"
	"log"
	"time"

	"github.com/Saad7890-web/rhombus/internal/outbox"
)

type Repository interface {
	ClaimBatch(ctx context.Context, workerID string, limit int, leaseDuration time.Duration, now time.Time) ([]outbox.Event, error)
	MarkDelivered(ctx context.Context, id string) error
	MarkRetryWait(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error
	MoveToDLQ(ctx context.Context, id string, lastError string) error
}

type Worker struct {
	workerID      string
	repo          Repository
	processor     Processor
	batchSize     int
	pollInterval  time.Duration
	leaseDuration time.Duration
	maxRetries    int
}

func NewWorker(
	workerID string,
	repo Repository,
	processor Processor,
	batchSize int,
	pollInterval time.Duration,
	leaseDuration time.Duration,
	maxRetries int,
) *Worker {
	return &Worker{
		workerID:      workerID,
		repo:          repo,
		processor:     processor,
		batchSize:     batchSize,
		pollInterval:  pollInterval,
		leaseDuration: leaseDuration,
		maxRetries:    maxRetries,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		if err := w.tick(ctx); err != nil {
			log.Printf("worker tick error: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) tick(ctx context.Context) error {
	now := time.Now().UTC()

	events, err := w.repo.ClaimBatch(ctx, w.workerID, w.batchSize, w.leaseDuration, now)
	if err != nil {
		return err
	}

	for _, e := range events {
		if err := w.handleEvent(ctx, e); err != nil {
			log.Printf("event processing failed id=%s err=%v", e.ID, err)
		}
	}

	return nil
}

func (w *Worker) handleEvent(ctx context.Context, e outbox.Event) error {
	err := w.processor.Process(ctx, e)
	if err == nil {
		return w.repo.MarkDelivered(ctx, e.ID)
	}

	nextRetryCount := e.RetryCount + 1
	if nextRetryCount > w.maxRetries {
		return w.repo.MoveToDLQ(ctx, e.ID, err.Error())
	}

	availableAt := time.Now().UTC().Add(backoff(nextRetryCount))
	return w.repo.MarkRetryWait(ctx, e.ID, nextRetryCount, availableAt, err.Error())
}

func backoff(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	base := time.Second
	return time.Duration(1<<uint(retryCount-1)) * base
}