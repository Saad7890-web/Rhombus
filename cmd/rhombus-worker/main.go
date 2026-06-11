package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Saad7890-web/rhombus/internal/config"
	kafkadelivery "github.com/Saad7890-web/rhombus/internal/delivery/kafka"
	"github.com/Saad7890-web/rhombus/internal/dispatcher"
	"github.com/Saad7890-web/rhombus/internal/observability"
	"github.com/Saad7890-web/rhombus/internal/outbox"
	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to create pg pool: %v", err)
	}
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	obs := observability.New("rhombus-worker")

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9091"
	}
	go func() {
		log.Printf("metrics listening on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, obs.MetricsHandler()); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server stopped: %v", err)
		}
	}()

	producerCfg := kafkadelivery.DefaultConfig()
	producerCfg.Brokers = cfg.KafkaBrokers
	producerCfg.ClientID = cfg.KafkaClientID

	producer, err := kafkadelivery.NewSegmentioProducer(producerCfg)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	processor := kafkadelivery.NewProcessor(
		producer,
		func(e outbox.Event) (string, error) {
			topic := e.EventType
			if cfg.KafkaTopicPrefix != "" {
				topic = cfg.KafkaTopicPrefix + topic
			}
			return topic, nil
		},
	)

	worker := dispatcher.NewWorker(
		cfg.WorkerID,
		repo,
		processor,
		cfg.BatchSize,
		cfg.PollInterval,
		cfg.LeaseDuration,
		cfg.MaxRetries,
	)
	worker.SetObserver(obs)

	log.Println("rhombus-worker starting...")

	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker stopped: %v", err)
	}

	time.Sleep(250 * time.Millisecond)
	log.Println("rhombus-worker shutdown complete")
}