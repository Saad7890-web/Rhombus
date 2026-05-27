package config

import (
	"errors"
	"os"
	"strconv"
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
		KafkaTopicPrefix: getenv("KAFKA_TOPIC_PREFIX", ""),
		WorkerID:        getenv("WORKER_ID", "worker-1"),
		BatchSize:       getenvInt("BATCH_SIZE", 100),
		PollInterval:    getenvDuration("POLL_INTERVAL", 2*time.Second),
		LeaseDuration:   getenvDuration("LEASE_DURATION", 30*time.Second),
		MaxRetries:      getenvInt("MAX_RETRIES", 5),
	}

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.KafkaBrokers = strings.Split(brokers, ",")
	}

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
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}