package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

type SegmentioProducer struct {
	writers map[string]*segmentio.Writer
	cfg     Config
}

func NewSegmentioProducer(cfg Config) (*SegmentioProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are required")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "rhombus"
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 10 * time.Millisecond
	}

	return &SegmentioProducer{
		writers: make(map[string]*segmentio.Writer),
		cfg:     cfg,
	}, nil
}

func (p *SegmentioProducer) getWriter(topic string) *segmentio.Writer {
	if w, ok := p.writers[topic]; ok {
		return w
	}

	w := &segmentio.Writer{
		Addr:         segmentio.TCP(p.cfg.Brokers...),
		Topic:        topic,
		Balancer:     &segmentio.Hash{},
		RequiredAcks: segmentio.RequireAll,
		Async:        p.cfg.Async,
		BatchTimeout: p.cfg.BatchTimeout,
	}

	p.writers[topic] = w
	return w
}

func (p *SegmentioProducer) Produce(
	ctx context.Context,
	topic string,
	key []byte,
	value []byte,
	headers map[string]string,
) error {
	if topic == "" {
		return errors.New("topic is required")
	}

	msgHeaders := make([]segmentio.Header, 0, len(headers))
	for k, v := range headers {
		msgHeaders = append(msgHeaders, segmentio.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	msg := segmentio.Message{
		Key:     key,
		Value:   value,
		Headers: msgHeaders,
		Time:    time.Now().UTC(),
	}

	return p.getWriter(topic).WriteMessages(ctx, msg)
}

func (p *SegmentioProducer) Close() error {
	var firstErr error
	for topic, w := range p.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close writer %s: %w", topic, err)
		}
	}
	return firstErr
}