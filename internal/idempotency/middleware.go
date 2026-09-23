package idempotency

import (
	"net/http"
)

// Middleware wraps an http.Handler with deduplication and request coalescing
// keyed by the Idempotency-Key header, keeping all state in an in-memory Store.
// It suits a single process; use PostgresMiddleware when side effects live in
// PostgreSQL and must survive crashes or span several instances.
type Middleware struct {
	store *Store
	cfg   config
}

// New returns a Middleware backed by store.
func New(store *Store, opts ...Option) *Middleware {
	return &Middleware{store: store, cfg: newConfig(opts)}
}

// Handler returns next wrapped with idempotency handling.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, hash, ok := m.cfg.admit(w, r, next)
		if !ok {
			return
		}

		e, oc := m.store.begin(key, hash)
		switch oc {
		case outcomeConflict:
			http.Error(w, HeaderName+" was already used for a different request", http.StatusUnprocessableEntity)
		case outcomeFollower:
			m.replayWhenReady(w, r, e)
		case outcomeLeader:
			m.runLeader(w, r, next, key)
		}
	})
}

// runLeader executes next exactly once for this key, caches the result unless it
// is a server error or the client vanished, and writes it to w.
func (m *Middleware) runLeader(w http.ResponseWriter, r *http.Request, next http.Handler, key string) {
	rec := newRecorder()

	func() {
		defer func() {
			if p := recover(); p != nil {
				// Release the key so followers get a retryable error and future
				// requests can try again, then re-raise for the server's own
				// recovery/logging to handle.
				m.store.abort(key)
				panic(p)
			}
		}()
		next.ServeHTTP(rec, r)
	}()

	res := rec.snapshot()

	switch {
	case r.Context().Err() != nil:
		// Client disconnected mid-flight; the captured response may be partial.
		m.store.abort(key)
	case res.status >= http.StatusInternalServerError:
		// Don't memoize transient server failures — let retries re-execute.
		m.store.abort(key)
	default:
		m.store.complete(key, res)
	}

	writeResponse(w, res, false)
}

// replayWhenReady blocks until the leader for e resolves, then replays its
// response. If the leader failed, or the caller's own context is cancelled
// first, it writes an appropriate error instead.
func (m *Middleware) replayWhenReady(w http.ResponseWriter, r *http.Request, e *entry) {
	select {
	case <-e.done:
	case <-r.Context().Done():
		http.Error(w, "client closed request while a duplicate was in flight", 499)
		return
	}

	if e.result == nil {
		http.Error(w, "the original request for this "+HeaderName+" failed; please retry", http.StatusBadGateway)
		return
	}
	writeResponse(w, e.result, true)
}
