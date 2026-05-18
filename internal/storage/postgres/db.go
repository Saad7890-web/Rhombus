package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

func NewDB(pool *pgxpool.Pool) *DB {
	return &DB{Pool: pool}
}

func (db *DB) Ping(ctx context.Context) error {
	return db.Pool.Ping(ctx)
}