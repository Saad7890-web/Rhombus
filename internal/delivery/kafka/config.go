package kafka

import "time"

type Config struct {
	Brokers      []string
	ClientID     string
	BatchTimeout time.Duration
	Async        bool
}

func DefaultConfig() Config {
	return Config{
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}
}