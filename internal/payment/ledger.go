// Package payment is a small demo REST API for a payment service. Its only
// purpose is to give the idempotency layer a handler with a real, observable
// side effect: every successful charge is written to a ledger, and tests count
// the recorded charges to prove exactly-once execution.
package payment

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// Charge is a single recorded movement of money.
type Charge struct {
	ID          string    `json:"id"`
	Account     string    `json:"account"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
}

// ErrNotFound is returned when a charge does not exist.
var ErrNotFound = errors.New("charge not found")

// Ledger records charges. Implementations must be safe for concurrent use.
type Ledger interface {
	// CreateCharge records c (ID and CreatedAt are assigned) and returns it.
	// Each successful call is one side effect.
	CreateCharge(ctx context.Context, c Charge) (Charge, error)
	// GetCharge returns the charge with the given id, or ErrNotFound.
	GetCharge(ctx context.Context, id string) (Charge, error)
}

// MemoryLedger is an in-memory Ledger for running the demo without a database.
type MemoryLedger struct {
	mu    sync.Mutex
	byID  map[string]Charge
	clock func() time.Time
}

// NewMemoryLedger returns an empty MemoryLedger.
func NewMemoryLedger() *MemoryLedger {
	return &MemoryLedger{byID: make(map[string]Charge), clock: time.Now}
}

// CreateCharge implements Ledger.
func (l *MemoryLedger) CreateCharge(_ context.Context, c Charge) (Charge, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c.ID = newID()
	c.CreatedAt = l.clock()
	l.byID[c.ID] = c
	return c, nil
}

// GetCharge implements Ledger.
func (l *MemoryLedger) GetCharge(_ context.Context, id string) (Charge, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.byID[id]
	if !ok {
		return Charge{}, ErrNotFound
	}
	return c, nil
}

// Count reports how many charges have been recorded.
func (l *MemoryLedger) Count(context.Context) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(len(l.byID)), nil
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "ch_" + hex.EncodeToString(b[:])
}
