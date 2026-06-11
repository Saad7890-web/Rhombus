package dispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/Saad7890-web/rhombus/internal/retry"
)

type fakeRepo struct {
	markDeliveredCalled bool
	markRetryCalled     bool
	moveToDLQCalled     bool
}

func (f *fakeRepo) ClaimBatch(ctx context.Context, workerID string, limit int, leaseDuration time.Duration, now time.Time) ([]outbox.Event, error) {
	return nil, nil
}

func (f *fakeRepo) MarkDelivered(ctx context.Context, id string) error {
	f.markDeliveredCalled = true
	return nil
}

func (f *fakeRepo) MarkRetryWait(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error {
	f.markRetryCalled = true
	return nil
}

func (f *fakeRepo) MoveToDLQ(ctx context.Context, id string, lastError string) error {
	f.moveToDLQCalled = true
	return nil
}

type fakeProcessor struct {
	err error
}

func (f fakeProcessor) Process(ctx context.Context, e outbox.Event) error {
	return f.err
}

func TestHandleEvent_RetryableErrorSchedulesRetry(t *testing.T) {
	repo := &fakeRepo{}
	worker := NewWorker("worker-1", repo, fakeProcessor{err: errors.New("temporary")}, 10, time.Second, 30*time.Second, 5)

	err := worker.handleEvent(context.Background(), outbox.Event{
		ID:         "event-1",
		RetryCount: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !repo.markRetryCalled {
		t.Fatal("expected retry update")
	}
	if repo.moveToDLQCalled {
		t.Fatal("did not expect DLQ")
	}
}

func TestHandleEvent_NonRetryableErrorMovesToDLQ(t *testing.T) {
	repo := &fakeRepo{}
	worker := NewWorker("worker-1", repo, fakeProcessor{err: retry.NonRetryable(errors.New("bad payload"))}, 10, time.Second, 30*time.Second, 5)

	err := worker.handleEvent(context.Background(), outbox.Event{
		ID:         "event-2",
		RetryCount: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !repo.moveToDLQCalled {
		t.Fatal("expected DLQ move")
	}
	if repo.markRetryCalled {
		t.Fatal("did not expect retry update")
	}
}