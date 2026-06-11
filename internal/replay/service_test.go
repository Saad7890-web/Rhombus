package replay

import (
	"context"
	"testing"
	"time"

	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
)

type fakeRepo struct {
	items []postgres.DLQItem
	item  *postgres.DLQItem
	newID string
}

func (f fakeRepo) ListDLQ(ctx context.Context, limit, offset int) ([]postgres.DLQItem, error) {
	return f.items, nil
}

func (f fakeRepo) GetDLQ(ctx context.Context, eventID string) (*postgres.DLQItem, error) {
	return f.item, nil
}

func (f fakeRepo) ReplayDLQ(ctx context.Context, eventID string, replayedBy string, notes string) (string, error) {
	return f.newID, nil
}

func TestReplayService(t *testing.T) {
	svc, err := New(fakeRepo{newID: "new-event-123"})
	if err != nil {
		t.Fatal(err)
	}

	res, err := svc.Replay(context.Background(), "dlq-1", "saad", "manual fix")
	if err != nil {
		t.Fatal(err)
	}

	if res.NewOutboxEventID != "new-event-123" {
		t.Fatalf("unexpected new id: %s", res.NewOutboxEventID)
	}
}

func TestReplayServiceList(t *testing.T) {
	now := time.Now().UTC()
	svc, err := New(fakeRepo{
		items: []postgres.DLQItem{
			{EventID: "dlq-1", CreatedAt: now},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	items, err := svc.List(context.Background(), 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}