package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type contextKey struct{}

type Collector struct {
	serviceName string
	startedAt   time.Time

	enqueued        atomic.Int64
	claimed         atomic.Int64
	delivered       atomic.Int64
	retryScheduled  atomic.Int64
	dlqMoved        atomic.Int64
	processingError atomic.Int64
	kafkaErrors     atomic.Int64
	dbErrors        atomic.Int64
	inflight        atomic.Int64

	dispatchBuckets []metricBucket
	kafkaBuckets    []metricBucket

	mu sync.Mutex
}

type metricBucket struct {
	upper float64
	count atomic.Int64
}

var defaultBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10,
}

func New(serviceName string) *Collector {
	c := &Collector{
		serviceName:   serviceName,
		startedAt:     time.Now().UTC(),
		dispatchBuckets: make([]metricBucket, 0, len(defaultBuckets)+1),
		kafkaBuckets:    make([]metricBucket, 0, len(defaultBuckets)+1),
	}

	for _, upper := range defaultBuckets {
		c.dispatchBuckets = append(c.dispatchBuckets, metricBucket{upper: upper})
		c.kafkaBuckets = append(c.kafkaBuckets, metricBucket{upper: upper})
	}
	c.dispatchBuckets = append(c.dispatchBuckets, metricBucket{upper: -1}) // +Inf
	c.kafkaBuckets = append(c.kafkaBuckets, metricBucket{upper: -1})        // +Inf

	return c
}

func NewTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		traceID = NewTraceID()
	}
	return context.WithValue(ctx, contextKey{}, traceID)
}

func TraceIDFromContext(ctx context.Context) string {
	v := ctx.Value(contextKey{})
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return ""
}

func TraceIDFromHeaders(headers map[string]string) string {
	if headers == nil {
		return ""
	}
	for _, key := range []string{"trace_id", "x-trace-id", "X-Trace-Id", "traceparent"} {
		if v := strings.TrimSpace(headers[key]); v != "" {
			return v
		}
	}
	return ""
}

func (c *Collector) IncEnqueued() {
	c.enqueued.Add(1)
}

func (c *Collector) IncClaimed(n int64) {
	if n > 0 {
		c.claimed.Add(n)
		c.inflight.Add(n)
	}
}

func (c *Collector) IncDelivered() {
	c.delivered.Add(1)
	c.inflight.Add(-1)
}

func (c *Collector) IncRetryScheduled() {
	c.retryScheduled.Add(1)
	c.inflight.Add(-1)
}

func (c *Collector) IncDLQMoved() {
	c.dlqMoved.Add(1)
	c.inflight.Add(-1)
}

func (c *Collector) IncProcessingError() {
	c.processingError.Add(1)
}

func (c *Collector) IncKafkaError() {
	c.kafkaErrors.Add(1)
}

func (c *Collector) IncDBError() {
	c.dbErrors.Add(1)
}

func (c *Collector) ObserveDispatch(d time.Duration) {
	observeBuckets(c.dispatchBuckets, d.Seconds())
}

func (c *Collector) ObserveKafkaProduce(d time.Duration) {
	observeBuckets(c.kafkaBuckets, d.Seconds())
}

func observeBuckets(buckets []metricBucket, seconds float64) {
	for i := range buckets {
		if buckets[i].upper < 0 {
			buckets[i].count.Add(1)
			return
		}
		if seconds <= buckets[i].upper {
			buckets[i].count.Add(1)
			return
		}
	}
}

func (c *Collector) Log(ctx context.Context, level string, msg string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		fields["trace_id"] = traceID
	}
	fields["service"] = c.serviceName

	var b strings.Builder
	b.WriteString("level=")
	b.WriteString(level)
	b.WriteString(" msg=")
	b.WriteString(msg)
	for k, v := range fields {
		b.WriteString(" ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(fmt.Sprint(v))
	}

	log.Println(b.String())
}

func (c *Collector) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		write := func(format string, args ...any) {
			_, _ = fmt.Fprintf(w, format, args...)
		}

		write("# HELP rhombus_events_enqueued_total Total number of outbox events enqueued.\n")
		write("# TYPE rhombus_events_enqueued_total counter\n")
		write("rhombus_events_enqueued_total{service=%q} %d\n", c.serviceName, c.enqueued.Load())

		write("# HELP rhombus_events_claimed_total Total number of events claimed by workers.\n")
		write("# TYPE rhombus_events_claimed_total counter\n")
		write("rhombus_events_claimed_total{service=%q} %d\n", c.serviceName, c.claimed.Load())

		write("# HELP rhombus_events_delivered_total Total number of events delivered successfully.\n")
		write("# TYPE rhombus_events_delivered_total counter\n")
		write("rhombus_events_delivered_total{service=%q} %d\n", c.serviceName, c.delivered.Load())

		write("# HELP rhombus_retry_scheduled_total Total number of events scheduled for retry.\n")
		write("# TYPE rhombus_retry_scheduled_total counter\n")
		write("rhombus_retry_scheduled_total{service=%q} %d\n", c.serviceName, c.retryScheduled.Load())

		write("# HELP rhombus_dlq_total Total number of events moved to DLQ.\n")
		write("# TYPE rhombus_dlq_total counter\n")
		write("rhombus_dlq_total{service=%q} %d\n", c.serviceName, c.dlqMoved.Load())

		write("# HELP rhombus_processing_errors_total Total number of processing errors.\n")
		write("# TYPE rhombus_processing_errors_total counter\n")
		write("rhombus_processing_errors_total{service=%q} %d\n", c.serviceName, c.processingError.Load())

		write("# HELP rhombus_kafka_errors_total Total number of Kafka publish errors.\n")
		write("# TYPE rhombus_kafka_errors_total counter\n")
		write("rhombus_kafka_errors_total{service=%q} %d\n", c.serviceName, c.kafkaErrors.Load())

		write("# HELP rhombus_db_errors_total Total number of database errors.\n")
		write("# TYPE rhombus_db_errors_total counter\n")
		write("rhombus_db_errors_total{service=%q} %d\n", c.serviceName, c.dbErrors.Load())

		write("# HELP rhombus_inflight_events Number of in-flight events.\n")
		write("# TYPE rhombus_inflight_events gauge\n")
		write("rhombus_inflight_events{service=%q} %d\n", c.serviceName, c.inflight.Load())

		write("# HELP rhombus_dispatch_duration_seconds Event dispatch duration histogram.\n")
		write("# TYPE rhombus_dispatch_duration_seconds histogram\n")
		c.writeBuckets(w, "rhombus_dispatch_duration_seconds", c.dispatchBuckets)

		write("# HELP rhombus_kafka_produce_duration_seconds Kafka publish duration histogram.\n")
		write("# TYPE rhombus_kafka_produce_duration_seconds histogram\n")
		c.writeBuckets(w, "rhombus_kafka_produce_duration_seconds", c.kafkaBuckets)

		write("# HELP rhombus_build_info Build information.\n")
		write("# TYPE rhombus_build_info gauge\n")
		write("rhombus_build_info{service=%q,started_at=%q} 1\n", c.serviceName, c.startedAt.Format(time.RFC3339Nano))
	})
}

func (c *Collector) writeBuckets(w http.ResponseWriter, name string, buckets []metricBucket) {
	var cumulative int64
	for _, b := range buckets {
		n := b.count.Load()
		cumulative += n

		le := "+Inf"
		if b.upper >= 0 {
			le = fmt.Sprintf("%g", b.upper)
		}

		_, _ = fmt.Fprintf(w, "%s_bucket{service=%q,le=%q} %d\n", name, c.serviceName, le, cumulative)
	}
	_, _ = fmt.Fprintf(w, "%s_count{service=%q} %d\n", name, c.serviceName, cumulative)
}