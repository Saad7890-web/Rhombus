package replay

import (
	"context"
	"errors"
	"time"

	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
)

type Repository interface {
	ListDLQ(ctx context.Context, limit, offset int) ([]postgres.DLQItem, error)
	GetDLQ(ctx context.Context, eventID string) (*postgres.DLQItem, error)
	ReplayDLQ(ctx context.Context, eventID string, replayedBy string, notes string) (string, error)
}

type Service struct {
	repo Repository
}

type ReplayResult struct {
	OriginalDLQEventID string    `json:"original_dlq_event_id"`
	NewOutboxEventID   string    `json:"new_outbox_event_id"`
	ReplayedAt         time.Time `json:"replayed_at"`
}

func New(repo Repository) (*Service, error) {
	if repo == nil {
		return nil, errors.New("replay repository is nil")
	}
	return &Service{repo: repo}, nil
}

func (s *Service) List(ctx context.Context, limit, offset int) ([]postgres.DLQItem, error) {
	return s.repo.ListDLQ(ctx, limit, offset)
}

func (s *Service) Get(ctx context.Context, eventID string) (*postgres.DLQItem, error) {
	return s.repo.GetDLQ(ctx, eventID)
}

func (s *Service) Replay(ctx context.Context, eventID string, replayedBy string, notes string) (*ReplayResult, error) {
	newID, err := s.repo.ReplayDLQ(ctx, eventID, replayedBy, notes)
	if err != nil {
		return nil, err
	}

	return &ReplayResult{
		OriginalDLQEventID: eventID,
		NewOutboxEventID:   newID,
		ReplayedAt:         time.Now().UTC(),
	}, nil
}