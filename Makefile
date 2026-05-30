test:
	go test ./... -count=1

test-integration:
	DATABASE_URL=postgres://rhombus:rhombus@localhost:5430/rhombus?sslmode=disable KAFKA_BROKERS=localhost:29092 go test ./tests -count=1 -v