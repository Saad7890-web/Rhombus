package tests

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	kafkadelivery "github.com/Saad7890-web/rhombus/internal/delivery/kafka"
	"github.com/Saad7890-web/rhombus/internal/dispatcher"
	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorker_EndToEnd_DeliversToKafka_AndMarksDelivered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	databaseURL := os.Getenv("DATABASE_URL")
	require.NotEmpty(t, databaseURL, "DATABASE_URL is required for integration tests")

	brokersEnv := os.Getenv("KAFKA_BROKERS")
	require.NotEmpty(t, brokersEnv, "KAFKA_BROKERS is required for integration tests")

	brokers := strings.Split(brokersEnv, ",")
	require.NotEmpty(t, brokers)

	err := waitForTCP(ctx, brokers[0], 60*time.Second)
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	_, _ = pool.Exec(ctx, `DELETE FROM rhombus_outbox`)

	topic := "rhombus-e2e-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	err = createTopicWithRetry(ctx, brokers[0], topic)
	require.NoError(t, err)

	producer, err := kafkadelivery.NewSegmentioProducer(kafkadelivery.Config{
		Brokers:  brokers,
		ClientID: "rhombus-e2e",
	})
	require.NoError(t, err)
	defer producer.Close()

	processor := kafkadelivery.NewProcessor(
		producer,
		func(e outbox.Event) (string, error) {
			return topic, nil
		},
	)

	worker := dispatcher.NewWorker(
		"worker-e2e",
		repo,
		processor,
		10,
		200*time.Millisecond,
		5*time.Second,
		3,
	)

	ev := &outbox.Event{
		AggregateType: "order",
		AggregateID:   "999",
		OrderingKey:   "order-999",
		EventType:     "orders.created",
		SchemaVersion: 1,
		Payload:       []byte(`{"order_id":"999","status":"created"}`),
		Metadata:      []byte(`{"source":"e2e-test"}`),
		Destination:   []byte(`{"kafka":{"topic":"` + topic + `"}}`),
	}

	err = repo.Insert(ctx, ev)
	require.NoError(t, err)

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	done := make(chan error, 1)
	go func() {
		done <- worker.Run(runCtx)
	}()

	reader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     "rhombus-e2e-reader-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		StartOffset: segmentio.FirstOffset,
	})
	defer reader.Close()

	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("order-999"), msg.Key)
	require.JSONEq(t, `{"order_id":"999","status":"created"}`, string(msg.Value))

	require.Eventually(t, func() bool {
		var status string
		err := pool.QueryRow(ctx, `SELECT status FROM rhombus_outbox WHERE id = $1`, ev.ID).Scan(&status)
		if err != nil {
			return false
		}
		return status == string(outbox.StatusDelivered)
	}, 10*time.Second, 200*time.Millisecond)

	runCancel()
	_ = <-done
}

