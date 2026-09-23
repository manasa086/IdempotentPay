package payment

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manasa086/IdempotentPay/internal/db"
)

// PostgresLedger stores charges in PostgreSQL. When called behind
// idempotency.PostgresMiddleware, CreateCharge joins the middleware's
// transaction (via db.Conn), so the charge commits atomically with its
// Idempotency-Key.
type PostgresLedger struct {
	pool *pgxpool.Pool
}

// NewPostgresLedger returns a PostgresLedger using pool.
func NewPostgresLedger(pool *pgxpool.Pool) *PostgresLedger {
	return &PostgresLedger{pool: pool}
}

// CreateCharge implements Ledger.
func (l *PostgresLedger) CreateCharge(ctx context.Context, c Charge) (Charge, error) {
	c.ID = newID()
	err := db.Conn(ctx, l.pool).QueryRow(ctx,
		`INSERT INTO charges (id, account, amount_cents, currency) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		c.ID, c.Account, c.AmountCents, c.Currency).Scan(&c.CreatedAt)
	if err != nil {
		return Charge{}, fmt.Errorf("insert charge: %w", err)
	}
	return c, nil
}

// GetCharge implements Ledger.
func (l *PostgresLedger) GetCharge(ctx context.Context, id string) (Charge, error) {
	var c Charge
	err := db.Conn(ctx, l.pool).QueryRow(ctx,
		`SELECT id, account, amount_cents, currency, created_at FROM charges WHERE id = $1`,
		id).Scan(&c.ID, &c.Account, &c.AmountCents, &c.Currency, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Charge{}, ErrNotFound
	} else if err != nil {
		return Charge{}, fmt.Errorf("get charge: %w", err)
	}
	return c, nil
}

// Count reports how many charges have been committed.
func (l *PostgresLedger) Count(ctx context.Context) (int64, error) {
	var n int64
	err := l.pool.QueryRow(ctx, `SELECT count(*) FROM charges`).Scan(&n)
	return n, err
}
