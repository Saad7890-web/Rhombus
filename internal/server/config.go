package server

import (
	"os"
	"time"
)

type Config struct {
	Address         string
	ReadinessTimeout time.Duration
	ShutdownTimeout  time.Duration
	ServiceName      string
	ServiceVersion   string
}

func LoadConfig() Config {
	return Config{
		Address:          getenv("SERVER_ADDR", ":8080"),
		ReadinessTimeout:  getenvDuration("SERVER_READINESS_TIMEOUT", 2*time.Second),
		ShutdownTimeout:   getenvDuration("SERVER_SHUTDOWN_TIMEOUT", 10*time.Second),
		ServiceName:       getenv("SERVICE_NAME", "rhombus-server"),
		ServiceVersion:    getenv("SERVICE_VERSION", "dev"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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