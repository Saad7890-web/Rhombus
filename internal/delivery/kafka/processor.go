package kafka

import (
	"context"
	"errors"
	"strconv"

	"github.com/Saad7890-web/rhombus/internal/outbox"
)

type Processor struct {
	producer      Producer
	topicResolver func(e outbox.Event) (string, error)
}

func NewProcessor(
	p Producer,
	resolver func(e outbox.Event) (string, error),
) *Processor {
	return &Processor{
		producer:      p,
		topicResolver: resolver,
	}
}

func (p *Processor) Process(ctx context.Context, e outbox.Event) error {
	if p.producer == nil {
		return errors.New("producer is nil")
	}

	topic, err := p.topicResolver(e)
	if err != nil {
		return err
	}

	headers := map[string]string{
		"event_id":       e.ID,
		"event_type":     e.EventType,
		"aggregate_type": e.AggregateType,
		"aggregate_id":   e.AggregateID,
		"schema_version": strconv.Itoa(e.SchemaVersion),
	}

	if e.TraceID != nil {
		headers["trace_id"] = *e.TraceID
	}
	if e.CorrelationID != nil {
		headers["correlation_id"] = *e.CorrelationID
	}
	if e.IdempotencyKey != nil {
		headers["idempotency_key"] = *e.IdempotencyKey
	}

	return p.producer.Produce(
		ctx,
		topic,
		[]byte(e.OrderingKey),
		e.Payload,
		headers,
	)
}