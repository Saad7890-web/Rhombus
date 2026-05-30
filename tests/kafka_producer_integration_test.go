package tests

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	kafkadelivery "github.com/Saad7890-web/rhombus/internal/delivery/kafka"
)

func TestSegmentioProducer_ProduceAndConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	brokersEnv := os.Getenv("KAFKA_BROKERS")
	require.NotEmpty(t, brokersEnv, "KAFKA_BROKERS is required for Kafka integration tests")

	brokers := strings.Split(brokersEnv, ",")
	require.NotEmpty(t, brokers)

	err := waitForTCP(ctx, brokers[0], 60*time.Second)
	require.NoError(t, err)

	topic := "rhombus-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	err = createTopicWithRetry(ctx, brokers[0], topic)
	require.NoError(t, err)

	producer, err := kafkadelivery.NewSegmentioProducer(kafkadelivery.Config{
		Brokers:  brokers,
		ClientID: "rhombus-test",
	})
	require.NoError(t, err)
	defer producer.Close()

	key := []byte("order-123")
	value := []byte(`{"order_id":"123","status":"created"}`)
	headers := map[string]string{
		"event_id":   "evt-123",
		"event_type": "orders.created",
	}

	err = producer.Produce(ctx, topic, key, value, headers)
	require.NoError(t, err)

	reader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     "rhombus-test-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		StartOffset: segmentio.FirstOffset,
	})
	defer reader.Close()

	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, key, msg.Key)
	require.Equal(t, value, msg.Value)

	foundEventID := false
	foundEventType := false
	for _, h := range msg.Headers {
		if h.Key == "event_id" && string(h.Value) == "evt-123" {
			foundEventID = true
		}
		if h.Key == "event_type" && string(h.Value) == "orders.created" {
			foundEventType = true
		}
	}

	require.True(t, foundEventID, "event_id header missing")
	require.True(t, foundEventType, "event_type header missing")
}

func waitForTCP(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("tcp not ready at %s: %w", addr, lastErr)
}

func createTopicWithRetry(ctx context.Context, broker string, topic string) error {
	deadline := time.Now().Add(45 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		conn, err := segmentio.Dial("tcp", broker)
		if err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
			}
			continue
		}

		lastErr = conn.CreateTopics(segmentio.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		_ = conn.Close()

		if lastErr == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("failed to create topic %s after retrying: %w", topic, lastErr)
}