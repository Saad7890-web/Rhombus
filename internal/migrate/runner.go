package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultSchemaMigrationsTable = "schema_migrations"

var migrationFilePattern = regexp.MustCompile(`^(\d+)_.*\.sql$`)

type Runner struct {
	pool      *pgxpool.Pool
	dir       string
	lockID    int64
	tableName string
}

type Migration struct {
	Version  int64
	Name     string
	Filename string
	Path     string
	SQL      []byte
	SHA256   string
}

type AppliedMigration struct {
	Version  int64
	Name     string
	Checksum string
}

func NewRunner(pool *pgxpool.Pool, dir string) *Runner {
	if dir == "" {
		dir = "migrations"
	}

	return &Runner{
		pool:      pool,
		dir:       dir,
		lockID:    914827364, 
		tableName: defaultSchemaMigrationsTable,
	}
}

func (r *Runner) Up(ctx context.Context) error {
	if r == nil || r.pool == nil {
		return errors.New("migrate runner is not initialized")
	}

	migrations, err := r.loadMigrations()
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return fmt.Errorf("no migration files found in %q", r.dir)
	}

	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire postgres connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, r.lockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, r.lockID)
	}()

	if err := r.ensureSchemaMigrationsTable(ctx, conn); err != nil {
		return err
	}

	applied, err := r.listApplied(ctx, conn)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if current, ok := applied[m.Version]; ok {
			if current.Checksum != m.SHA256 {
				return fmt.Errorf(
					"migration %d (%s) already applied but checksum changed: db=%s file=%s",
					m.Version, m.Name, current.Checksum, m.SHA256,
				)
			}
			continue
		}

		if err := r.applyOne(ctx, conn, m); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) ensureSchemaMigrationsTable(ctx context.Context, conn *pgxpool.Conn) error {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`, r.tableName)

	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create %s table: %w", r.tableName, err)
	}
	return nil
}

func (r *Runner) listApplied(ctx context.Context, conn *pgxpool.Conn) (map[int64]AppliedMigration, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT version, name, checksum FROM %s ORDER BY version`, r.tableName))
	if err != nil {
		return nil, fmt.Errorf("list applied migrations: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]AppliedMigration)
	for rows.Next() {
		var m AppliedMigration
		if err := rows.Scan(&m.Version, &m.Name, &m.Checksum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		out[m.Version] = m
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}

	return out, nil
}

func (r *Runner) applyOne(ctx context.Context, conn *pgxpool.Conn, m Migration) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction for migration %d (%s): %w", m.Version, m.Name, err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if _, err := tx.Exec(ctx, string(m.SQL)); err != nil {
		return fmt.Errorf("execute migration %d (%s): %w", m.Version, m.Name, err)
	}

	_, err = tx.Exec(
		ctx,
		fmt.Sprintf(`INSERT INTO %s (version, name, checksum, applied_at) VALUES ($1, $2, $3, NOW())`, r.tableName),
		m.Version,
		m.Name,
		m.SHA256,
	)
	if err != nil {
		return fmt.Errorf("record migration %d (%s): %w", m.Version, m.Name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d (%s): %w", m.Version, m.Name, err)
	}

	return nil
}

func (r *Runner) loadMigrations() ([]Migration, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %q: %w", r.dir, err)
	}

	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		match := migrationFilePattern.FindStringSubmatch(name)
		if match == nil {
			continue
		}

		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version from %q: %w", name, err)
		}

		path := filepath.Join(r.dir, name)
		sqlBytes, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read migration file %q: %w", path, err)
		}

		sum := sha256.Sum256(sqlBytes)

		migrations = append(migrations, Migration{
			Version:  version,
			Name:     strings.TrimSuffix(name, ".sql"),
			Filename: name,
			Path:     path,
			SQL:      sqlBytes,
			SHA256:   hex.EncodeToString(sum[:]),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		if migrations[i].Version == migrations[j].Version {
			return migrations[i].Filename < migrations[j].Filename
		}
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}