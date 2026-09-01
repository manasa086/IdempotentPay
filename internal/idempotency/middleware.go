package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
)

// HeaderName is the request header carrying the client-chosen idempotency token.
const HeaderName = "Idempotency-Key"

// defaultMaxBodyBytes caps how much request body the middleware will buffer in
// order to fingerprint it and forward it to the handler.
const defaultMaxBodyBytes = 1 << 20 // 1 MiB

// Middleware wraps an http.Handler with deduplication and request coalescing
// keyed by the Idempotency-Key header. Construct one with New.
type Middleware struct {
	store       *Store
	guarded     map[string]bool
	maxBodyByte int64
	requireKey  bool
}

// Option configures a Middleware.
type Option func(*Middleware)

// GuardMethods sets which HTTP methods are subject to idempotency handling.
// Methods are matched case-insensitively. The default set is POST, PUT, PATCH,
// DELETE; safe methods (GET, HEAD, OPTIONS) always pass straight through.
func GuardMethods(methods ...string) Option {
	return func(m *Middleware) {
		m.guarded = make(map[string]bool, len(methods))
		for _, x := range methods {
			m.guarded[strings.ToUpper(x)] = true
		}
	}
}

// MaxBodyBytes limits how many bytes of the request body are buffered. Requests
// whose body exceeds the limit are rejected with 413. The default is 1 MiB.
func MaxBodyBytes(n int64) Option {
	return func(m *Middleware) { m.maxBodyByte = n }
}

// RequireKey controls what happens when a guarded request arrives without an
// Idempotency-Key header. When true (the default), the request is rejected with
// 400. When false, it is passed through unguarded.
func RequireKey(required bool) Option {
	return func(m *Middleware) { m.requireKey = required }
}

// New returns a Middleware backed by store.
func New(store *Store, opts ...Option) *Middleware {
	m := &Middleware{
		store:       store,
		guarded:     map[string]bool{http.MethodPost: true, http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true},
		maxBodyByte: defaultMaxBodyBytes,
		requireKey:  true,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// Handler returns next wrapped with idempotency handling.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !m.guarded[r.Method] {
			next.ServeHTTP(w, r)
			return
		}

		key := strings.TrimSpace(r.Header.Get(HeaderName))
		if key == "" {
			if m.requireKey {
				http.Error(w, HeaderName+" header is required for "+r.Method+" requests", http.StatusBadRequest)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		body, err := readAtMost(r.Body, m.maxBodyByte)
		if errors.Is(err, errBodyTooLarge) {
			http.Error(w, "request body exceeds the limit accepted by the idempotency layer", http.StatusRequestEntityTooLarge)
			return
		} else if err != nil {
			http.Error(w, "could not read request body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		hash := fingerprint(r.Method, r.URL.Path, key, body)

		e, oc := m.store.begin(key, hash)
		switch oc {
		case outcomeConflict:
			http.Error(w, HeaderName+" was already used for a different request", http.StatusUnprocessableEntity)
			return

		case outcomeFollower:
			m.replayWhenReady(w, r, e)
			return

		case outcomeLeader:
			m.runLeader(w, r, next, key)
			return
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

// fingerprint derives a stable identity for a request so that reusing a key with
// a different method, path, or body is detected as a conflict. The key itself is
// mixed in so fingerprints can't collide across keys.
func fingerprint(method, path, key string, body []byte) string {
	h := sha256.New()
	writeField(h, method)
	writeField(h, path)
	writeField(h, key)
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func writeField(h io.Writer, s string) {
	_, _ = io.WriteString(h, s)
	_, _ = h.Write([]byte{0})
}

var errBodyTooLarge = errors.New("request body too large")

// readAtMost reads up to limit bytes from rc. It returns errBodyTooLarge if the
// body has more than limit bytes. A limit <= 0 means unlimited.
func readAtMost(rc io.ReadCloser, limit int64) ([]byte, error) {
	if rc == nil {
		return nil, nil
	}
	defer rc.Close()
	if limit <= 0 {
		return io.ReadAll(rc)
	}
	body, err := io.ReadAll(io.LimitReader(rc, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errBodyTooLarge
	}
	return body, nil
}
