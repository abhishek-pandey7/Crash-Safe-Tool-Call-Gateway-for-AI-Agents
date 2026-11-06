// Package storetest gives each test its own Postgres schema.
//
// Tests that need Postgres read GATEWAY_TEST_DATABASE_URL and are skipped
// when it is unset. scripts/test-db starts a suitable container.
package storetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

// EnvURL names the environment variable holding the test database URL.
const EnvURL = "GATEWAY_TEST_DATABASE_URL"

// URL returns a database URL whose connections use a fresh, empty schema
// that is dropped when the test ends. Separate processes can use it too.
func URL(t testing.TB) string {
	t.Helper()
	base := os.Getenv(EnvURL)
	if base == "" {
		t.Skipf("%s not set; skipping Postgres test", EnvURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "t_" + hex.EncodeToString(b[:])

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("storetest: connect: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("storetest: create schema: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if c, err := pgx.Connect(ctx, base); err == nil {
			_, _ = c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
			_ = c.Close(ctx)
		}
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("storetest: parse url: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

// New returns a migrated store in a fresh schema.
func New(t testing.TB) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), URL(t))
	if err != nil {
		t.Fatalf("storetest: open: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}
