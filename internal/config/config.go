package config

import (
	"errors"
	"os"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL     string
	KafkaBrokers     []string
	KafkaClientID    string
	KafkaTopicPrefix string
	WorkerID        string
	BatchSize       int
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	MaxRetries      int
}

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		KafkaClientID:   getenv("KAFKA_CLIENT_ID", "rhombus"),
		KafkaTopicPrefix: os.Getenv("KAFKA_TOPIC_PREFIX"),
		WorkerID:        getenv("WORKER_ID", "worker-1"),
	}

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.KafkaBrokers = strings.Split(brokers, ",")
	}

	cfg.BatchSize = getenvInt("BATCH_SIZE", 100)
	cfg.PollInterval = getenvDuration("POLL_INTERVAL", 2*time.Second)
	cfg.LeaseDuration = getenvDuration("LEASE_DURATION", 30*time.Second)
	cfg.MaxRetries = getenvInt("MAX_RETRIES", 5)

	if cfg.DatabaseURL == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS is required")
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	return fallback
}