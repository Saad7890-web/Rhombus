package kafka

import "context"

type Producer interface {
	Produce(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error
	Close() error
}