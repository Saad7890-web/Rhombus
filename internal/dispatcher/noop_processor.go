package dispatcher

import (
	"context"

	"github.com/Saad7890-web/rhombus/internal/outbox"
)

type NoopProcessor struct{}

func (p *NoopProcessor) Process(ctx context.Context, e outbox.Event) error {
	return nil
}