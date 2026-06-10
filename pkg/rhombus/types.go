package rhombus

import "github.com/Saad7890-web/rhombus/internal/outbox"

type Event = outbox.Event
type Status = outbox.Status

const (
	StatusPending    = outbox.StatusPending
	StatusProcessing = outbox.StatusProcessing
	StatusDelivered  = outbox.StatusDelivered
	StatusRetryWait  = outbox.StatusRetryWait
	StatusFailed     = outbox.StatusFailed
	StatusDLQ        = outbox.StatusDLQ
)