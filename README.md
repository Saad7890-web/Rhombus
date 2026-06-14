# Rhombus

**Consistency orchestration for event-driven systems.**

Rhombus is an embedded Go library and worker process that solves the dual-write problem between your database and your message broker. When you write business data, Rhombus writes your events in the same transaction — then reliably delivers them to Kafka with at-least-once guarantees, per-aggregate ordering, and full replayability.

---

## How it works

```
Your App ──► [ DB Transaction ]
               ├── INSERT orders …
               └── INSERT outbox_events …
                         │
                    [ Rhombus Worker ]
                         │
                         ▼
                  [ Kafka Topic ]
                         │
                         ▼
               [ Downstream Consumers ]
```

Rhombus never delivers an event that wasn't committed, and never loses an event that was. The database transaction is the single source of truth.

---

## Guarantees

| Property                                      | Status |
| --------------------------------------------- | ------ |
| Atomic DB write + event persistence           | ✅     |
| At-least-once delivery                        | ✅     |
| Per-aggregate ordering                        | ✅     |
| Idempotent processing at application boundary | ✅     |
| Replayability via DLQ API                     | ✅     |
| Crash recovery                                | ✅     |
| Eventual consistency downstream               | ✅     |

**Not in scope:**

- Global distributed transactions
- True exactly-once across all systems
- Cross-database atomicity

---

## Prerequisites

- Go 1.25+
- Docker + Docker Compose
- Postgres and Kafka (local or remote)

---

## Quickstart

**1. Start infrastructure**

```bash
docker compose up -d
```

**2. Run migrations**

Apply the outbox migration against your local Postgres instance:

```bash
make migrate
```

**3. Run tests**

```bash
make test
```

**4. Start the server**

```bash
export DATABASE_URL='postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable'

go run ./cmd/rhombus-server
```

**5. Start the worker**

```bash
export DATABASE_URL='postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable'
export KAFKA_BROKERS='localhost:29092'

go run ./cmd/rhombus-worker
```

**6. Run the sample app**

```bash
export DATABASE_URL='postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable'

go run ./examples/sample-app
```

---

## Usage

### Embedded library

Wrap your business writes with `WithTransaction`. Rhombus will enqueue the event atomically within the same database transaction.

```go
client, err := rhombus.New(pool)
if err != nil {
    return err
}

err = client.WithTransaction(ctx, func(tx *rhombus.Transaction) error {
    _, err := tx.Exec(
        `INSERT INTO orders (id, customer_id, amount_cents) VALUES ($1, $2, $3)`,
        orderID,
        customerID,
        amountCents,
    )
    if err != nil {
        return err
    }

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

If the transaction rolls back, the event is discarded. If it commits, Rhombus guarantees delivery.

---

## Replay API

Rhombus exposes HTTP endpoints to inspect and replay dead-letter events.

| Method | Endpoint              | Description                        |
| ------ | --------------------- | ---------------------------------- |
| `GET`  | `/api/dlq`            | List all dead-letter events        |
| `GET`  | `/api/dlq/:id`        | Inspect a single dead-letter event |
| `POST` | `/api/dlq/:id/replay` | Replay a dead-letter event         |

---

## Configuration

Copy `.env.example` and adjust values for your environment:

```bash
cp .env.example .env
```

```bash
# Database
DATABASE_URL=postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable

# Kafka
KAFKA_BROKERS=localhost:29092
KAFKA_CLIENT_ID=rhombus
KAFKA_TOPIC_PREFIX=

# Worker
WORKER_ID=worker-1
BATCH_SIZE=100
POLL_INTERVAL=2s
LEASE_DURATION=30s
MAX_RETRIES=5

# Server
SERVER_ADDR=:8080
SERVER_READINESS_TIMEOUT=2s
SERVER_SHUTDOWN_TIMEOUT=10s

# Observability
METRICS_ADDR=:9091
SERVICE_NAME=rhombus-server
SERVICE_VERSION=dev

# Sample app
APP_ADDR=:8090
```

| Variable                   | Description                                      | Default          |
| -------------------------- | ------------------------------------------------ | ---------------- |
| `DATABASE_URL`             | Postgres connection string                       | —                |
| `KAFKA_BROKERS`            | Comma-separated broker list                      | —                |
| `KAFKA_CLIENT_ID`          | Kafka client identifier                          | `rhombus`        |
| `KAFKA_TOPIC_PREFIX`       | Optional prefix for all topics                   | —                |
| `WORKER_ID`                | Unique identifier for this worker instance       | `worker-1`       |
| `BATCH_SIZE`               | Events processed per poll cycle                  | `100`            |
| `POLL_INTERVAL`            | How often the worker polls the outbox            | `2s`             |
| `LEASE_DURATION`           | How long a worker holds a lock on an event batch | `30s`            |
| `MAX_RETRIES`              | Retry attempts before moving to DLQ              | `5`              |
| `SERVER_ADDR`              | HTTP server listen address                       | `:8080`          |
| `SERVER_READINESS_TIMEOUT` | Timeout for readiness checks                     | `2s`             |
| `SERVER_SHUTDOWN_TIMEOUT`  | Graceful shutdown window                         | `10s`            |
| `METRICS_ADDR`             | Prometheus metrics listen address                | `:9091`          |
| `SERVICE_NAME`             | Service name for traces and metrics              | `rhombus-server` |
| `SERVICE_VERSION`          | Service version tag                              | `dev`            |
| `APP_ADDR`                 | Sample app listen address                        | `:8090`          |

---

## Repository layout

```
cmd/
  rhombus-server/        # HTTP + metrics server entrypoint
  rhombus-worker/        # Outbox poll + Kafka dispatch entrypoint
examples/
  sample-app/            # End-to-end usage example
internal/
  config/                # Environment config loading
  dispatcher/            # Kafka dispatch logic
  observability/         # Metrics + tracing setup
  outbox/                # Outbox poll, lease, and retry logic
  replay/                # DLQ replay orchestration
  server/                # HTTP handler wiring
  storage/postgres/      # Postgres outbox queries
migrations/              # SQL migrations
pkg/
  rhombus/               # Public embedded library API
tests/                   # Integration tests
ui/
  dashboard/             # Observability dashboard
```

---

## Roadmap

- [x] Postgres outbox
- [x] Kafka delivery
- [x] Retry scheduling
- [x] DLQ storage and replay API
- [x] Observability (metrics + tracing)
- [x] Embedded Go library
- [ ] Redis destination support
- [ ] Elasticsearch destination support
- [ ] Dashboard UI (in progress)

---
