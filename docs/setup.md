# Rhombus

## Setup

### Local dependencies

- Postgres
- Kafka
- Go 1.25+

### Docker Compose

Start the local stack:

```bash
docker compose up -d
```

### Database migration

Apply the outbox migration before running the server or worker.

## Running the services

### Server

```bash
export DATABASE_URL='postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable'
go run ./cmd/rhombus-server
```

### Worker

```bash
export DATABASE_URL='postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable'
export KAFKA_BROKERS='localhost:29092'
go run ./cmd/rhombus-worker
```

### Dashboard

```bash
cd ui/dashboard
npm install
npm run dev
```

### Sample app

```bash
export DATABASE_URL='postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable'
go run ./examples/sample-app
```
