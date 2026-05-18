# ◆ Rhombus

> **Exactly-once, ordered event delivery across Postgres, Kafka, Redis, and Elasticsearch — without distributed transactions.**

Rhombus is a lightweight sidecar and embeddable library that solves the hardest problem in event-driven systems: keeping your database, message broker, cache, and search index provably in sync. It implements the Transactional Outbox pattern with a unified multi-destination router, idempotency engine, and built-in observability.

```
[Your App] ──INSERT──▶ [rhombus.outbox] ──▶ [Rhombus Core]
                                                    │
                              ┌─────────────────────┼─────────────────────┐
                              ▼                     ▼                     ▼
                           [Kafka]               [Redis]         [Elasticsearch]
                        (event streams)       (cache / TTL)     (search indices)
```

---

## The Problem

Every system using Postgres + Kafka + Redis + Elasticsearch eventually hits the same wall: **data inconsistency at the boundary**.

| Scenario                                      | What Goes Wrong                                             |
| --------------------------------------------- | ----------------------------------------------------------- |
| Write to Postgres, Kafka produce fails        | Search index goes stale. Other services miss the event.     |
| Update Redis cache, DB transaction rolls back | Cache holds data that was never actually committed.         |
| Produce Kafka event, DB commit fails          | Downstream services believe something happened that didn't. |

Existing solutions — Debezium, Kafka Connect, hand-rolled outbox code — are fragmented, operationally heavy, and lack a unified API. You end up stitching together three different systems to solve one coherent problem.

Rhombus treats this as a single, solvable problem.

---

## How It Works

Rhombus implements the **Transactional Outbox pattern** at the infrastructure level, so your application code stays clean.

1. **Your app writes a row** to `rhombus.outbox` inside the same Postgres transaction as your business data. If the transaction rolls back, the event never existed.

2. **Rhombus detects the commit** via Postgres `LISTEN/NOTIFY` (no polling lag, no missed events).

3. **Rhombus routes the event** to one or more destinations — Kafka, Redis, Elasticsearch — in the correct order, with exactly-once delivery semantics.

4. **Idempotency is enforced** at every destination using a combination of Redis locks and DB-side deduplication keys, so crashes and retries are safe.

5. **Failed events land in a Dead Letter Queue** with full context, viewable and replayable via the included dashboard.

---

## Key Features

### ◆ Unified Outbox Table Manager

Rhombus automatically provisions and manages `rhombus.outbox` in your existing Postgres instance. No new databases. No separate schema migrations to maintain. Your application writes one `INSERT`; Rhombus handles everything downstream.

```sql
INSERT INTO rhombus.outbox (aggregate_type, aggregate_id, event_type, payload, destinations)
VALUES ('order', 'ord_9f3k2', 'order.placed', '{"total": 149.99}', ARRAY['kafka', 'elasticsearch']);
```

### ◆ Multi-Destination Router

A single outbox event can fan out to multiple destinations simultaneously:

- **Kafka** — with Avro or Protobuf schema registry support, configurable topic routing, and partition key control
- **Redis** — stream appends or key/hash upserts with optional TTL synchronization
- **Elasticsearch** — idempotent document upserts with configurable index routing and refresh policy

### ◆ Idempotent Delivery Engine

Rhombus tracks every event across its full delivery lifecycle. Using a combination of Redis distributed locks and Postgres deduplication records, it guarantees:

- Events are never delivered twice, even after a crash mid-flight
- Events are delivered in the order they were committed
- Partial failures (e.g., Kafka succeeds but ES fails) are individually retried without re-sending to already-successful destinations

### ◆ Dead Letter Queue + Replay Dashboard

Failed events don't disappear. They land in Rhombus's DLQ with full diagnostic context: the original payload, destination, error, stack trace, and retry history. The included web UI lets you:

- Browse and filter failed events
- Inspect the exact error from each destination
- Edit the payload before replaying (for data-fix scenarios)
- Replay to any subset of the original destinations

### ◆ OpenTelemetry Observability

Every event produces a distributed trace spanning the full delivery chain:

```
DB commit → outbox insert → NOTIFY received → destination routed → Kafka ACK / Redis SET / ES indexed
```

Traces, metrics, and logs export to any OTLP-compatible backend: Jaeger, Grafana Tempo, Honeycomb, Datadog.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Your Application                    │
│                                                          │
│   BEGIN;                                                 │
│   UPDATE orders SET status = 'placed' WHERE id = $1;    │
│   INSERT INTO rhombus.outbox (...) VALUES (...);  ◀─────┼── one INSERT, that's it
│   COMMIT;                                                │
└─────────────────────┬───────────────────────────────────┘
                      │ Postgres LISTEN/NOTIFY
                      ▼
┌─────────────────────────────────────────────────────────┐
│                    Rhombus Core (Go)                     │
│                                                          │
│  ┌─────────────┐   ┌──────────────┐   ┌─────────────┐  │
│  │  Outbox     │   │  Idempotency │   │  Router     │  │
│  │  Watcher    │──▶│  Engine      │──▶│  Engine     │  │
│  └─────────────┘   └──────────────┘   └──────┬──────┘  │
│                                               │          │
│                          ┌────────────────────┤          │
│                          ▼         ▼          ▼          │
│                      [Kafka]   [Redis]    [ES]           │
│                                                          │
│  ┌─────────────────────────────────────────────────┐    │
│  │              DLQ + Replay Store                  │    │
│  └─────────────────────────────────────────────────┘    │
│                          │                               │
│                          ▼                               │
│              [Replay Dashboard (React)]                  │
└─────────────────────────────────────────────────────────┘
```

### Deployment Modes

**Sidecar mode** — Rhombus runs as a separate process alongside your application. Communicates via gRPC or HTTP. Language-agnostic; works with any app that can write to Postgres.

**Embedded library mode** — Import Rhombus directly into your Go application. Zero network hops for the outbox write path.

---

## Getting Started

### Prerequisites

- Postgres 14+
- Go 1.22+ (for building from source)
- Docker (for the quickstart)

### Quickstart (Docker Compose)

```bash
git clone https://github.com/your-org/rhombus.git
cd rhombus
docker compose up
```

This starts Rhombus alongside Postgres, Kafka, Redis, and Elasticsearch with a pre-configured demo environment.

### Install the CLI

```bash
go install github.com/your-org/rhombus/cmd/rhombus@latest
```

### Initialize Rhombus in Your Database

```bash
rhombus init --postgres "postgres://user:pass@localhost:5432/mydb"
```

This creates the `rhombus` schema, the `outbox` table, and installs the NOTIFY trigger. It's idempotent — safe to run on every deploy.

### Configure Destinations

```yaml
# rhombus.yaml
postgres:
  dsn: "postgres://user:pass@localhost:5432/mydb"

destinations:
  kafka:
    brokers: ["localhost:9092"]
    schema_registry: "http://localhost:8081"
    default_topic: "rhombus.events"
    topic_routing:
      order.*: "orders-topic"
      payment.*: "payments-topic"

  redis:
    addr: "localhost:6379"
    default_ttl: "24h"

  elasticsearch:
    addresses: ["http://localhost:9200"]
    default_index: "rhombus-events"
    index_routing:
      product.*: "products"
      user.*: "users"

dead_letter_queue:
  enabled: true
  max_retries: 5
  retry_backoff: "exponential"

observability:
  otlp_endpoint: "http://localhost:4317"
```

### Run the Sidecar

```bash
rhombus start --config rhombus.yaml
```

### Write Your First Event

From your application, in your normal Postgres transaction:

```sql
BEGIN;

-- Your business logic
INSERT INTO orders (id, user_id, total, status)
VALUES ('ord_9f3k2', 'usr_abc', 149.99, 'placed');

-- Tell Rhombus to fan this out
INSERT INTO rhombus.outbox (
  aggregate_type,
  aggregate_id,
  event_type,
  payload,
  destinations,
  idempotency_key
) VALUES (
  'order',
  'ord_9f3k2',
  'order.placed',
  '{"order_id": "ord_9f3k2", "user_id": "usr_abc", "total": 149.99}',
  ARRAY['kafka', 'elasticsearch'],
  'ord_9f3k2-placed-1'
);

COMMIT;
-- Rhombus picks this up via NOTIFY and routes it. Your transaction is done.
```

---

## Go Library Usage (Embedded Mode)

```go
import "github.com/your-org/rhombus"

func PlaceOrder(ctx context.Context, db *pgxpool.Pool, order Order) error {
    rh, err := rhombus.New(rhombus.Config{
        Postgres: db,
        Kafka:    rhombus.KafkaConfig{Brokers: []string{"localhost:9092"}},
        Redis:    rhombus.RedisConfig{Addr: "localhost:6379"},
    })
    if err != nil {
        return err
    }

    return db.BeginFunc(ctx, func(tx pgx.Tx) error {
        // Your business write
        _, err := tx.Exec(ctx, `INSERT INTO orders ...`, order.ID, order.Total)
        if err != nil {
            return err
        }

        // Outbox write — same transaction
        return rh.Outbox(tx).Publish(ctx, rhombus.Event{
            AggregateType:  "order",
            AggregateID:    order.ID,
            EventType:      "order.placed",
            Payload:        order,
            Destinations:   []string{"kafka", "elasticsearch"},
            IdempotencyKey: order.ID + "-placed",
        })
    })
    // If the transaction commits, the event will be delivered.
    // If it rolls back, the event never existed.
}
```

---

## Replay Dashboard

Access the dashboard at `http://localhost:7070` after starting the sidecar.

The dashboard provides:

- **Event stream view** — live tail of recently processed events with delivery status per destination
- **DLQ browser** — filter by aggregate type, event type, destination, error class, or time range
- **Event inspector** — full payload, headers, retry history, and per-destination delivery receipts
- **Replay controls** — select failed events, optionally edit the payload, and replay to any destination
- **Metrics** — throughput, latency percentiles, error rates, and DLQ depth over time

---

## Observability

Rhombus emits the following out of the box:

**Traces (OpenTelemetry)**

- `rhombus.outbox.received` — event picked up from outbox table
- `rhombus.route.kafka` / `rhombus.route.redis` / `rhombus.route.elasticsearch` — per-destination spans with outcome
- `rhombus.idempotency.check` — deduplication lookup timing

**Metrics (Prometheus)**

| Metric                             | Description                                      |
| ---------------------------------- | ------------------------------------------------ |
| `rhombus_events_received_total`    | Events received from outbox                      |
| `rhombus_events_delivered_total`   | Successful deliveries, by destination            |
| `rhombus_events_failed_total`      | Failed deliveries, by destination and error type |
| `rhombus_dlq_depth`                | Current DLQ depth                                |
| `rhombus_delivery_latency_seconds` | End-to-end delivery latency histogram            |

---

## Guarantees and Limitations

### What Rhombus Guarantees

- **At-least-once delivery** from outbox to each destination, with idempotency enforcement providing effective exactly-once semantics at the application level
- **Ordering** — events for the same `aggregate_id` are delivered in commit order
- **No phantom events** — an event only exists in the outbox if the business transaction committed
- **Crash safety** — Rhombus can crash and restart at any point; no events are lost or duplicated

### What Rhombus Does Not Guarantee

- **Sub-millisecond latency** — the outbox pattern adds a small delivery lag (typically 5–50ms depending on Postgres NOTIFY latency). This is not a real-time streaming system.
- **Cross-destination atomicity** — if Kafka succeeds and Elasticsearch fails, they are retried independently. There is no two-phase commit across destinations. Rhombus's idempotency layer ensures each destination eventually converges.
- **Schema evolution** — Rhombus routes payloads but does not manage Avro/Protobuf schema compatibility. Use your schema registry's compatibility rules for that.

---

## Configuration Reference

Full configuration reference: [docs/configuration.md](docs/configuration.md)

| Key                           | Type     | Default       | Description                      |
| ----------------------------- | -------- | ------------- | -------------------------------- |
| `postgres.dsn`                | string   | —             | Postgres connection string       |
| `postgres.outbox_schema`      | string   | `rhombus`     | Schema for the outbox table      |
| `kafka.brokers`               | []string | —             | Kafka broker addresses           |
| `kafka.schema_registry`       | string   | —             | Confluent Schema Registry URL    |
| `redis.addr`                  | string   | —             | Redis address                    |
| `elasticsearch.addresses`     | []string | —             | Elasticsearch node addresses     |
| `dlq.max_retries`             | int      | `5`           | Max delivery attempts before DLQ |
| `dlq.retry_backoff`           | string   | `exponential` | `linear` or `exponential`        |
| `observability.otlp_endpoint` | string   | —             | OTLP gRPC endpoint               |
| `server.grpc_port`            | int      | `7071`        | gRPC sidecar port                |
| `server.http_port`            | int      | `7070`        | HTTP API + dashboard port        |

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

**Development setup:**

```bash
git clone https://github.com/your-org/rhombus.git
cd rhombus
make dev-deps      # starts Postgres, Kafka, Redis, ES via Docker
make test          # runs the full test suite
make build         # builds the rhombus binary
```

**Project structure:**

```
rhombus/
├── cmd/rhombus/        # CLI entrypoint
├── core/
│   ├── outbox/         # Outbox watcher and NOTIFY listener
│   ├── router/         # Multi-destination routing engine
│   ├── idempotency/    # Deduplication and locking
│   └── dlq/            # Dead letter queue store
├── destinations/
│   ├── kafka/          # Kafka producer adapter
│   ├── redis/          # Redis adapter (streams + keys)
│   └── elasticsearch/  # ES adapter
├── api/
│   ├── grpc/           # gRPC server
│   └── http/           # HTTP API
├── dashboard/          # React replay dashboard
├── docs/               # Documentation
└── tests/integration/  # Integration test suite
```

---

## Roadmap

- [ ] **v0.1** — Core outbox + Kafka routing + idempotency engine
- [ ] **v0.2** — Redis and Elasticsearch destinations
- [ ] **v0.3** — DLQ + Replay Dashboard
- [ ] **v0.4** — OpenTelemetry integration
- [ ] **v0.5** — Embedded Go library mode
- [ ] **v1.0** — Production hardening, Helm chart, operator pattern

---

## License

Apache 2.0. See [LICENSE](LICENSE).

---

<p align="center">
  Built for engineers who are tired of debugging why the cache disagrees with the database.
</p>
