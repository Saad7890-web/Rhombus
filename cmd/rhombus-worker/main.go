package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Saad7890-web/rhombus/internal/dispatcher"
	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}
	defer pool.Close()

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)
	processor := &dispatcher.NoopProcessor{}

	worker := dispatcher.NewWorker(
		"worker-1",
		repo,
		processor,
		100,
		2*time.Second,
		30*time.Second,
		5,
	)

	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker stopped with error: %v", err)
	}
}