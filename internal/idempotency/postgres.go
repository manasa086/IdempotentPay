package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manasa086/IdempotentPay/internal/db"
)

// PostgresMiddleware is the crash-safe counterpart of Middleware. It records each
// Idempotency-Key in PostgreSQL inside the same transaction as the handler's
// side effects, so a key, its charge, and its cached response are committed
// together or not at all:
//
//   - Crash before commit: PostgreSQL rolls the whole transaction back. There is
//     no charge and no key, so the client's retry executes cleanly.
//   - Crash after commit, before the response reaches the client: the retry
//     finds the committed key and replays the stored response. No second charge.
//
// Duplicates are coalesced at two levels. Within a process they wait on the
// leader's in-memory flight. Across processes, the leader's uncommitted INSERT of
// the key holds the primary-key lock, so a duplicate's INSERT on another
// instance blocks until the leader commits (then replays) or rolls back (then
// executes).
//
// The handler must write its side effects through db.Conn(r.Context(), pool) so
// they join the transaction. Side effects outside the database (for example a
// call to an external processor) are not covered by the transaction.
type PostgresMiddleware struct {
	pool    *pgxpool.Pool
	cfg     config
	flights flightGroup
}

// NewPostgres returns a PostgresMiddleware storing keys in pool's database. The
// schema from db.Migrate must already be applied.
func NewPostgres(pool *pgxpool.Pool, opts ...Option) *PostgresMiddleware {
	return &PostgresMiddleware{pool: pool, cfg: newConfig(opts)}
}

// Handler returns next wrapped with idempotency handling.
func (m *PostgresMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, hash, ok := m.cfg.admit(w, r, next)
		if !ok {
			return
		}

		f, leader := m.flights.join(key, hash)
		if !leader {
			m.follow(w, r, f, hash)
			return
		}

		var out flightResult // zero value (nothing durable) if execute panics
		defer func() { m.flights.land(key, f, out) }()
		out = m.execute(r, next, key, hash)

		if out.conflict {
			writeConflict(w)
			return
		}
		writeResponse(w, out.res, out.replayed)
	})
}

// follow waits for the in-process leader and shares its outcome.
func (m *PostgresMiddleware) follow(w http.ResponseWriter, r *http.Request, f *flight, hash string) {
	if f.hash != hash {
		writeConflict(w)
		return
	}
	select {
	case <-f.done:
	case <-r.Context().Done():
		http.Error(w, "client closed request while a duplicate was in flight", 499)
		return
	}
	switch {
	case f.out.conflict:
		writeConflict(w)
	case !f.out.durable:
		http.Error(w, "the original request for this "+HeaderName+" failed; please retry", http.StatusBadGateway)
	default:
		writeResponse(w, f.out.res, true)
	}
}

// execute runs the leader's transaction: reserve the key, run the handler inside
// the transaction, store the response, commit. If the key is already committed
// it returns the stored response instead of running the handler.
func (m *PostgresMiddleware) execute(r *http.Request, next http.Handler, key, hash string) flightResult {
	// A disconnecting client must not abort a leader: duplicates may be waiting
	// on it, and the client's own retry will replay whatever it commits.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), m.cfg.timeout)
	defer cancel()

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return storeFailure("begin", err)
	}
	// Rolls back on every path that doesn't commit — including a handler panic,
	// which then continues to unwind. After a successful Commit it is a no-op.
	defer tx.Rollback(context.WithoutCancel(ctx))

	// Reserve the key. If another transaction holds an uncommitted row for it,
	// this blocks until that transaction commits (0 rows: replay) or rolls back
	// (1 row: we are now the leader).
	tag, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
		key, hash)
	if err != nil {
		return storeFailure("reserve key", err)
	}
	if tag.RowsAffected() == 0 {
		return replayCommitted(ctx, tx, key, hash)
	}

	rec := newRecorder()
	next.ServeHTTP(rec, r.WithContext(db.WithTx(ctx, tx)))
	m.cfg.phase(PhaseExecuted)

	res := rec.snapshot()
	if res.status >= http.StatusInternalServerError {
		// Don't memoize server errors. The deferred rollback discards any side
		// effects the handler wrote and releases the key for a retry.
		return flightResult{res: res}
	}

	header, err := json.Marshal(res.header)
	if err != nil {
		return storeFailure("encode response header", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE idempotency_keys SET response_status = $2, response_header = $3, response_body = $4 WHERE key = $1`,
		key, res.status, header, res.body); err != nil {
		return storeFailure("store response", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return storeFailure("commit", err)
	}
	m.cfg.phase(PhaseCommitted)

	return flightResult{res: res, durable: true}
}

// replayCommitted loads the stored response for a key that another transaction
// has already committed.
func replayCommitted(ctx context.Context, tx pgx.Tx, key, hash string) flightResult {
	var (
		storedHash string
		status     *int
		header     []byte
		body       []byte
	)
	err := tx.QueryRow(ctx,
		`SELECT request_hash, response_status, response_header, response_body FROM idempotency_keys WHERE key = $1`,
		key).Scan(&storedHash, &status, &header, &body)
	if err != nil {
		return storeFailure("load stored response", err)
	}
	if storedHash != hash {
		return flightResult{conflict: true, durable: true}
	}
	if status == nil {
		return storeFailure("load stored response", errors.New("committed key has no response"))
	}

	res := &response{status: *status, header: http.Header{}, body: body}
	if len(header) > 0 {
		if err := json.Unmarshal(header, &res.header); err != nil {
			return storeFailure("decode stored header", err)
		}
	}
	return flightResult{res: res, replayed: true, durable: true}
}

// storeFailure logs a database error and returns a retryable 503.
func storeFailure(op string, err error) flightResult {
	log.Printf("idempotency: %s: %v", op, err)
	return flightResult{res: errorResponse(http.StatusServiceUnavailable, "idempotency store unavailable; please retry")}
}

func writeConflict(w http.ResponseWriter) {
	http.Error(w, HeaderName+" was already used for a different request", http.StatusUnprocessableEntity)
}
