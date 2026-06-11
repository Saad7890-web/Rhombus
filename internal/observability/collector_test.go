package observability

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTraceIDRoundTrip(t *testing.T) {
	ctx := WithTraceID(context.Background(), "trace-123")
	got := TraceIDFromContext(ctx)
	if got != "trace-123" {
		t.Fatalf("expected trace-123, got %q", got)
	}
}

func TestMetricsHandlerOutputsPrometheusText(t *testing.T) {
	c := New("rhombus-test")
	c.IncEnqueued()
	c.IncClaimed(2)
	c.IncDelivered()
	c.ObserveDispatch(12 * time.Millisecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	c.MetricsHandler().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "rhombus_events_enqueued_total") {
		t.Fatal("missing enqueued metric")
	}
	if !strings.Contains(body, "rhombus_dispatch_duration_seconds") {
		t.Fatal("missing dispatch histogram")
	}
}