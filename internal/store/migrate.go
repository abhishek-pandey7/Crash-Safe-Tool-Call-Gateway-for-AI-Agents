package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate applies any embedded migrations not yet recorded in
// schema_migrations. Instances starting together are serialised by an
// advisory lock, so each migration runs exactly once.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)

	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('crashsafe_gateway_migrations'))`); err != nil {
			return fmt.Errorf("store: migration lock: %w", err)
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
			version int PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return fmt.Errorf("store: migrations table: %w", err)
		}
		for _, name := range names {
			version, err := migrationVersion(name)
			if err != nil {
				return err
			}
			var done bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
				return err
			}
			if done {
				continue
			}
			sql, err := migrationFiles.ReadFile(name)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("store: migration %s: %w", name, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
				return err
			}
		}
		return nil
	})
}

func migrationVersion(path string) (int, error) {
	base := path[strings.LastIndex(path, "/")+1:]
	prefix, _, ok := strings.Cut(base, "_")
	if !ok {
		return 0, fmt.Errorf("store: migration %s has no version prefix", path)
	}
	return strconv.Atoi(prefix)
}
