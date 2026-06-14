package rhombus

import (
	"context"
	"errors"
	"fmt"

	"github.com/Saad7890-web/rhombus/internal/storage/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txInserter interface {
	InsertTx(ctx context.Context, tx pgx.Tx, e *Event) error
}

type Client struct {
	pool *pgxpool.Pool
	repo txInserter
}

type Transaction struct {
	ctx    context.Context
	tx     pgx.Tx
	repo   txInserter
	closed bool
}

func New(pool *pgxpool.Pool) (*Client, error) {
	if pool == nil {
		return nil, errors.New("pgx pool is nil")
	}

	db := postgres.NewDB(pool)
	repo := postgres.NewOutboxRepository(db)

	return &Client{
		pool: pool,
		repo: repo,
	}, nil
}

func (c *Client) BeginTransaction(ctx context.Context) (*Transaction, error) {
	return c.BeginTransactionWithOptions(ctx, pgx.TxOptions{})
}

func (c *Client) BeginTransactionWithOptions(ctx context.Context, opts pgx.TxOptions) (*Transaction, error) {
	if c == nil || c.pool == nil {
		return nil, errors.New("client is not initialized")
	}

	tx, err := c.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return &Transaction{
		ctx:  ctx,
		tx:   tx,
		repo: c.repo,
	}, nil
}

func (c *Client) WithTransaction(ctx context.Context, fn func(tx *Transaction) error) (err error) {
	tx, err := c.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if !tx.closed {
			_ = tx.Rollback()
		}
	}()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (c *Client) EnqueueEvent(ctx context.Context, tx pgx.Tx, event *Event) error {
	if c == nil || c.repo == nil {
		return errors.New("client is not initialized")
	}
	if tx == nil {
		return errors.New("transaction is nil")
	}
	if event == nil {
		return errors.New("event is nil")
	}
	return c.repo.InsertTx(ctx, tx, event)
}

func (t *Transaction) EnqueueEvent(event *Event) error {
	if t == nil {
		return errors.New("transaction is nil")
	}
	if t.closed {
		return errors.New("transaction is already closed")
	}
	if event == nil {
		return errors.New("event is nil")
	}
	return t.repo.InsertTx(t.ctx, t.tx, event)
}

func (t *Transaction) Exec(query string, args ...any) (pgconn.CommandTag, error) {
	if t == nil {
		return pgconn.CommandTag{}, errors.New("transaction is nil")
	}
	if t.closed {
		return pgconn.CommandTag{}, errors.New("transaction is already closed")
	}
	return t.tx.Exec(t.ctx, query, args...)
}

func (t *Transaction) QueryRow(query string, args ...any) pgx.Row {
	if t == nil {
		return nil
	}
	return t.tx.QueryRow(t.ctx, query, args...)
}

func (t *Transaction) Query(query string, args ...any) (pgx.Rows, error) {
	if t == nil {
		return nil, errors.New("transaction is nil")
	}
	if t.closed {
		return nil, errors.New("transaction is already closed")
	}
	return t.tx.Query(t.ctx, query, args...)
}

func (t *Transaction) Commit() error {
	if t == nil {
		return errors.New("transaction is nil")
	}
	if t.closed {
		return errors.New("transaction is already closed")
	}
	t.closed = true
	return t.tx.Commit(t.ctx)
}

func (t *Transaction) Rollback() error {
	if t == nil {
		return errors.New("transaction is nil")
	}
	if t.closed {
		return nil
	}
	t.closed = true
	return t.tx.Rollback(t.ctx)
}