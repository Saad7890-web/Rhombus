package dispatcher

import (
	"context"

	"github.com/Saad7890-web/rhombus/internal/outbox"
)

type Processor interface {
	Process(ctx context.Context, e outbox.Event) error
}