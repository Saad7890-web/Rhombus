package dispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Saad7890-web/rhombus/internal/outbox"
)

type failingRepo struct {
	deliveredCalled bool
	retryCalled     bool
	dlqCalled       bool
}

func (f *failingRepo) ClaimBatch(ctx context.Context, workerID string, limit int, leaseDuration time.Duration, now time.Time) ([]outbox.Event, error) {
	return nil, nil
}

func (f *failingRepo) MarkDelivered(ctx context.Context, id string) error {
	f.deliveredCalled = true
	return nil
}

func (f *failingRepo) MarkRetryWait(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error {
	f.retryCalled = true
	return nil
}

func (f *failingRepo) MoveToDLQ(ctx context.Context, id string, lastError string) error {
	f.dlqCalled = true
	return nil
}

type alwaysFailProcessor struct{}

func (p alwaysFailProcessor) Process(ctx context.Context, e outbox.Event) error {
	return errors.New("temporary failure")
}

func TestWorker_HandleEvent_MovesToDLQAfterMaxRetries(t *testing.T) {
	repo := &failingRepo{}
	worker := NewWorker(
		"worker-hardening",
		repo,
		alwaysFailProcessor{},
		10,
		time.Second,
		30*time.Second,
		0,
	)

	err := worker.handleEvent(context.Background(), outbox.Event{
		ID:         "event-dlq",
		RetryCount: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !repo.dlqCalled {
		t.Fatal("expected DLQ move")
	}
	if repo.retryCalled {
		t.Fatal("did not expect retry scheduling")
	}
	if repo.deliveredCalled {
		t.Fatal("did not expect delivery mark")
	}
}

func TestWorker_BackoffGrowsExponentially(t *testing.T) {
	tests := []struct {
		retryCount int
		want       time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
	}

	for _, tt := range tests {
		got := backoff(tt.retryCount)
		if got != tt.want {
			t.Fatalf("retryCount=%d want=%s got=%s", tt.retryCount, tt.want, got)
		}
	}
}