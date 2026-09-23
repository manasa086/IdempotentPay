// Package db holds the PostgreSQL plumbing shared by the idempotency layer and
// the payment handler: opening a pool, applying the schema, and carrying an open
// transaction through a request's context.
//
// The transaction hand-off is what makes the Postgres-backed idempotency layer
// crash-safe. The layer begins a transaction, reserves the Idempotency-Key in it,
// and passes it to the handler through the context. The handler writes its side
// effect (the charge) with Conn(ctx, pool), which picks up that transaction, and
// the layer then stores the response and commits — so the key, the charge, and
// the cached response become durable together or not at all.
package db

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// Querier is the subset of pgx shared by *pgxpool.Pool and pgx.Tx, so data-access
// code can run either inside or outside a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Open connects to the database at url and verifies the connection.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Migrate creates the tables the service needs if they don't already exist.
func Migrate(ctx context.Context, q Querier) error {
	if _, err := q.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

type txKey struct{}

// WithTx returns a context that carries tx, so code further down the call chain
// joins the caller's transaction instead of opening its own.
func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// Conn returns the transaction carried by ctx, or fallback if there is none.
func Conn(ctx context.Context, fallback Querier) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return fallback
}
