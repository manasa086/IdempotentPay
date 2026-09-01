// Package payment is a small demo REST API for a payment service. Its only
// purpose is to give the idempotency layer a handler with a real, observable
// side effect: every successful charge mutates an in-memory ledger and bumps a
// counter that tests assert against to prove exactly-once execution.
package payment

import (
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

// Ledger is a goroutine-safe in-memory record of charges.
type Ledger struct {
	mu        sync.Mutex
	byID      map[string]Charge
	order     []string
	processed int64
	clock     func() time.Time
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger {
	return &Ledger{byID: make(map[string]Charge), clock: time.Now}
}

// ErrInvalidCharge is returned for a charge request that fails validation.
var ErrInvalidCharge = errors.New("invalid charge")

// Charge validates and records a new charge. Each call that returns nil error is
// one side effect: it appends to the ledger and increments the processed
// counter. The idempotency layer must ensure this runs at most once per key.
func (l *Ledger) Charge(account string, amountCents int64, currency string) (Charge, error) {
	if account == "" || amountCents <= 0 || len(currency) != 3 {
		return Charge{}, ErrInvalidCharge
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	c := Charge{
		ID:          newID(),
		Account:     account,
		AmountCents: amountCents,
		Currency:    currency,
		CreatedAt:   l.clock(),
	}
	l.byID[c.ID] = c
	l.order = append(l.order, c.ID)
	l.processed++
	return c, nil
}

// Get returns the charge with the given id.
func (l *Ledger) Get(id string) (Charge, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.byID[id]
	return c, ok
}

// Processed reports how many charges have been recorded. Tests use this to
// verify that N concurrent duplicate requests produced exactly one charge.
func (l *Ledger) Processed() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.processed
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "ch_" + hex.EncodeToString(b[:])
}
