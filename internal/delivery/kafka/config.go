package kafka

import "time"

type Config struct {
	Brokers         []string
	ClientID        string
	TopicPrefix     string
	RequiredAcks    int
	BatchTimeout    time.Duration
	Async           bool
	AllowAutoCreate bool
}

func DefaultConfig() Config {
	return Config{
		RequiredAcks:    -1, 
		BatchTimeout:    10 * time.Millisecond,
		Async:           false,
		AllowAutoCreate: false,
	}
}