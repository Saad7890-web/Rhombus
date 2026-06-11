package retry

import (
	"errors"
	"testing"
	"time"
)

func TestPolicyDelay(t *testing.T) {
	p := DefaultPolicy()

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
		got := p.Delay(tt.retryCount)
		if got != tt.want {
			t.Fatalf("retryCount=%d: want %s, got %s", tt.retryCount, tt.want, got)
		}
	}
}

func TestPolicyCapsAtMaxDelay(t *testing.T) {
	p := DefaultPolicy()

	got := p.Delay(20)
	if got != 10*time.Minute {
		t.Fatalf("expected cap at 10m, got %s", got)
	}
}

func TestNonRetryableClassification(t *testing.T) {
	err := errors.New("boom")
	wrapped := NonRetryable(err)

	if IsRetryable(wrapped) {
		t.Fatal("expected non-retryable error")
	}

	if !IsRetryable(err) {
		t.Fatal("expected normal error to be retryable")
	}
}