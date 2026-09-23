// Package dbtest gives each test its own freshly migrated PostgreSQL schema.
//
// Tests that need a database call NewSchema. If TEST_DATABASE_URL is not set the
// test is skipped, so `go test ./...` still passes on machines without Postgres;
// CI sets the variable and runs them against a real server.
package dbtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manasa086/IdempotentPay/internal/db"
)

// EnvVar names the environment variable holding the test database URL.
const EnvVar = "TEST_DATABASE_URL"

// Schema is an isolated, migrated schema for a single test.
type Schema struct {
	// URL connects to the database with search_path set to this schema, so it
	// can be handed to a separate server process.
	URL string
	// Pool is connected with the same search_path.
	Pool *pgxpool.Pool
}

// NewSchema creates a uniquely named schema, applies the service schema to it,
// and drops it when the test finishes.
func NewSchema(t testing.TB) *Schema {
	t.Helper()
	base := os.Getenv(EnvVar)
	if base == "" {
		t.Skipf("%s not set; skipping PostgreSQL test", EnvVar)
	}
	ctx := context.Background()

	admin, err := db.Open(ctx, base)
	if err != nil {
		t.Fatalf("connect to %s: %v", EnvVar, err)
	}
	defer admin.Close()

	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "test_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse %s: %v", EnvVar, err)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()

	pool, err := db.Open(ctx, u.String())
	if err != nil {
		t.Fatalf("connect to test schema: %v", err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		admin, err := db.Open(context.Background(), base)
		if err != nil {
			t.Logf("drop schema %s: %v", name, err)
			return
		}
		defer admin.Close()
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+name+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", name, err)
		}
	})
	return &Schema{URL: u.String(), Pool: pool}
}
