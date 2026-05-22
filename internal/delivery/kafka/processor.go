package kafka

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Saad7890-web/rhombus/internal/outbox"
)

type Processor struct {
	producer Producer
	topicResolver func(e outbox.Event) (string, error)
}

func NewProcessor(p Producer, resolver func(e outbox.Event) (string, error)) *Processor {
	return &Processor{
		producer: p,
		topicResolver: resolver,
	}
}

func (p *Processor) Process(ctx context.Context, e outbox.Event) error {
	if p.producer == nil {
		return errors.New("kafka producer is nil")
	}

	topic, err := p.topicResolver(e)
	if err != nil {
		return err
	}

	key := []byte(e.OrderingKey)
	value := e.Payload

	headers := map[string]string{
		"event_id":        e.ID,
		"event_type":      e.EventType,
		"aggregate_type":  e.AggregateType,
		"aggregate_id":    e.AggregateID,
		"schema_version":   string(rune(e.SchemaVersion)),
	}

	if len(e.Metadata) > 0 {
		var meta map[string]any
		if err := json.Unmarshal(e.Metadata, &meta); err == nil {
			_ = meta
		}
	}

	return p.producer.Produce(ctx, topic, key, value, headers)
}