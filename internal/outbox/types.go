package outbox

import "time"

type Status string

const (
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusDelivered  Status = "DELIVERED"
	StatusRetryWait  Status = "RETRY_WAIT"
	StatusFailed     Status = "FAILED"
	StatusDLQ        Status = "DLQ"
)

type Event struct {
	ID             string
	TenantID       *string
	AggregateType  string
	AggregateID    string
	OrderingKey    string
	EventType      string
	SchemaVersion  int
	Payload        []byte
	Metadata       []byte
	Destination    []byte
	Status         Status
	RetryCount     int
	LastError      *string
	AvailableAt    time.Time
	LeasedUntil    *time.Time
	LeasedBy       *string
	TraceID        *string
	CorrelationID  *string
	IdempotencyKey *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ProcessedAt    *time.Time
}