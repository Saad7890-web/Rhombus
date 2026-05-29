test:
	go test ./... -count=1

test-integration:
	DATABASE_URL=postgres://rhombus:rhombus@localhost:5432/rhombus?sslmode=disable go test ./tests -count=1 -v