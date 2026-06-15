# Rhombus

## Installation

```bash
go get github.com/your-org/rhombus
```

Requires Go 1.21+ and PostgreSQL 14+.

---

## Quickstart

### 1. Run migrations

```bash
rhombus migrate --database-url "$DATABASE_URL"
```

This creates the `rhombus_outbox` and `rhombus_dlq` tables.

### 2. Wrap your writes

```go
err := client.WithTransaction(ctx, func(tx *rhombus.Transaction) error {
    // Your business write
    _, err := tx.Exec(
        `INSERT INTO orders (id, customer_id, amount_cents) VALUES ($1, $2, $3)`,
        orderID,
        customerID,
        amountCents,
    )
    if err != nil {
        return err
    }

    // Enqueue the outbox event — same transaction, same commit
    return tx.EnqueueEvent(&rhombus.Event{
        AggregateType: "order",
        AggregateID:   orderID,
        OrderingKey:   orderID,
        EventType:     "orders.created",
        SchemaVersion: 1,
        Payload:       payload,
        Destination:   []byte(`{"kafka":{"topic":"orders.created"}}`),
    })
})
```

If either the business write or the enqueue fails, the entire transaction rolls back. No orphaned events, no missed publishes.

### 3. Start the relay

```go
relay := rhombus.NewRelay(client, dispatcher)
relay.Start(ctx)
```

The relay polls the outbox table and forwards events to their destinations. Confirmed events are deleted; failed events are moved to the DLQ after configurable retry exhaustion.

---

## Dead-Letter Queue

Events that exceed the retry limit are written to the DLQ with full context for inspection and replay.

### Replay a DLQ event

```bash
curl -X POST http://localhost:8080/api/dlq/<event_id>/replay \
  -H 'Content-Type: application/json' \
  -d '{"replayed_by":"operator","notes":"fixed payload"}'
```

The replayed event re-enters the outbox pipeline from scratch. The original DLQ record is retained with replay metadata for audit purposes.

---

## Event Schema

| Field           | Type     | Description                                                        |
| --------------- | -------- | ------------------------------------------------------------------ |
| `AggregateType` | `string` | The domain entity type (e.g. `"order"`, `"user"`)                  |
| `AggregateID`   | `string` | The entity's unique identifier                                     |
| `OrderingKey`   | `string` | Used to partition events for ordering (typically the aggregate ID) |
| `EventType`     | `string` | Dot-separated event name (e.g. `"orders.created"`)                 |
| `SchemaVersion` | `int`    | Payload schema version for consumer compatibility                  |
| `Payload`       | `[]byte` | Serialized event body                                              |
| `Destination`   | `[]byte` | JSON routing config (see Destinations)                             |

---

## Destinations

Routing is configured per-event via the `Destination` field.

**Kafka**

```json
{ "kafka": { "topic": "orders.created" } }
```

**Webhook**

```json
{ "webhook": { "url": "https://example.com/hooks/orders" } }
```

Multiple destinations per event are supported.

---

## Configuration

```go
client, err := rhombus.NewClient(rhombus.Config{
    DatabaseURL:     os.Getenv("DATABASE_URL"),
    PollInterval:    2 * time.Second,
    MaxRetries:      5,
    RetryBackoff:    rhombus.ExponentialBackoff(500*time.Millisecond, 30*time.Second),
    BatchSize:       100,
})
```

---

## Running Tests

Tests require a live PostgreSQL instance. The CI config below shows the expected setup.

```bash
DATABASE_URL=postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable \
  go test ./... -count=1
```

### CI (GitHub Actions)

```yaml
name: ci

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: rhombus
          POSTGRES_PASSWORD: rhombus
          POSTGRES_DB: rhombus
        ports:
          - 5432:5432
        options: >-
          --health-cmd="pg_isready -U rhombus -d rhombus"
          --health-interval=5s
          --health-timeout=5s
          --health-retries=20

    env:
      DATABASE_URL: postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable

    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.25"
      - run: go mod download
      - run: go test ./... -count=1
```

---

## How It Works

```
Your Service
    │
    ▼
┌──────────────────────────────┐
│         DB Transaction        │
│  ┌─────────────┐             │
│  │ Business    │             │
│  │ Write       │             │
│  └─────────────┘             │
│  ┌─────────────┐             │
│  │ Outbox      │             │
│  │ INSERT      │             │
│  └─────────────┘             │
└──────────────────────────────┘
    │ commit (atomic)
    ▼
┌─────────────┐
│  Outbox     │
│  Table      │◄── Relay polls
└─────────────┘
    │ forward
    ▼
┌─────────────┐   ┌─────────────┐
│   Kafka     │   │   Webhook   │  ...
└─────────────┘   └─────────────┘
    │ on failure (after N retries)
    ▼
┌─────────────┐
│     DLQ     │◄── /api/dlq/:id/replay
└─────────────┘
```

---
