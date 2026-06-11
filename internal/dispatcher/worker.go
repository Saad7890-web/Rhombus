package dispatcher

import (
	"context"
	"log"
	"time"

	"github.com/Saad7890-web/rhombus/internal/observability"
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
	obs           *observability.Collector
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

func (w *Worker) SetObserver(obs *observability.Collector) {
	w.obs = obs
}

func (w *Worker) Run(ctx context.Context) error {
	if w.obs != nil {
		w.obs.Log(ctx, "info", "worker starting", map[string]any{
			"worker_id": w.workerID,
		})
	}

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		if err := w.tick(ctx); err != nil {
			log.Printf("worker tick error: %v", err)
			if w.obs != nil {
				w.obs.IncDBError()
				w.obs.Log(ctx, "error", "worker tick error", map[string]any{
					"worker_id": w.workerID,
					"error":     err.Error(),
				})
			}
		}

		select {
		case <-ctx.Done():
			if w.obs != nil {
				w.obs.Log(ctx, "info", "worker shutting down", map[string]any{
					"worker_id": w.workerID,
				})
			}
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

	if w.obs != nil && len(events) > 0 {
		w.obs.IncClaimed(int64(len(events)))
		w.obs.Log(ctx, "info", "claimed batch", map[string]any{
			"worker_id": w.workerID,
			"count":     len(events),
		})
	}

	for _, e := range events {
		if err := w.handleEvent(ctx, e); err != nil {
			log.Printf("event processing failed id=%s err=%v", e.ID, err)
		}
	}

	return nil
}

func (w *Worker) handleEvent(ctx context.Context, e outbox.Event) error {
	traceID := e.ID
	if e.TraceID != nil && *e.TraceID != "" {
		traceID = *e.TraceID
	}
	ctx = observability.WithTraceID(ctx, traceID)

	start := time.Now()
	err := w.processor.Process(ctx, e)
	if w.obs != nil {
		w.obs.ObserveDispatch(time.Since(start))
	}

	if err == nil {
		if w.obs != nil {
			w.obs.IncDelivered()
			w.obs.Log(ctx, "info", "event delivered", map[string]any{
				"event_id":       e.ID,
				"aggregate_type": e.AggregateType,
				"aggregate_id":   e.AggregateID,
				"event_type":     e.EventType,
				"worker_id":      w.workerID,
			})
		}
		return w.repo.MarkDelivered(ctx, e.ID)
	}

	if w.obs != nil {
		w.obs.IncProcessingError()
		w.obs.Log(ctx, "error", "event processing failed", map[string]any{
			"event_id":       e.ID,
			"aggregate_type": e.AggregateType,
			"aggregate_id":   e.AggregateID,
			"event_type":     e.EventType,
			"worker_id":      w.workerID,
			"error":          err.Error(),
		})
	}

	nextRetryCount := e.RetryCount + 1
	if nextRetryCount > w.maxRetries {
		if w.obs != nil {
			w.obs.IncDLQMoved()
			w.obs.Log(ctx, "error", "event moved to dlq", map[string]any{
				"event_id":       e.ID,
				"aggregate_type": e.AggregateType,
				"aggregate_id":   e.AggregateID,
				"event_type":     e.EventType,
				"worker_id":      w.workerID,
				"error":          err.Error(),
			})
		}
		return w.repo.MoveToDLQ(ctx, e.ID, err.Error())
	}

	availableAt := time.Now().UTC().Add(backoff(nextRetryCount))
	if w.obs != nil {
		w.obs.IncRetryScheduled()
		w.obs.Log(ctx, "info", "event scheduled for retry", map[string]any{
			"event_id":       e.ID,
			"aggregate_type": e.AggregateType,
			"aggregate_id":   e.AggregateID,
			"event_type":     e.EventType,
			"worker_id":      w.workerID,
			"retry_count":    nextRetryCount,
			"available_at":   availableAt.Format(time.RFC3339Nano),
			"error":          err.Error(),
		})
	}
	return w.repo.MarkRetryWait(ctx, e.ID, nextRetryCount, availableAt, err.Error())
}

func backoff(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	base := time.Second
	return time.Duration(1<<uint(retryCount-1)) * base
}