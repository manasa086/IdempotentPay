package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// HeaderName is the request header carrying the client-chosen idempotency token.
const HeaderName = "Idempotency-Key"

// defaultMaxBodyBytes caps how much request body the middleware will buffer in
// order to fingerprint it and forward it to the handler.
const defaultMaxBodyBytes = 1 << 20 // 1 MiB

// defaultExecutionTimeout bounds how long a Postgres-backed leader may run.
const defaultExecutionTimeout = 30 * time.Second

// Phase marks a point in a Postgres-backed leader's execution. Phases exist so
// tests (and the demo server's failpoint) can crash the process at the exact
// moments that matter for exactly-once guarantees.
type Phase string

const (
	// PhaseExecuted: the handler has returned and its side effects are written
	// inside the transaction, but nothing has been committed yet.
	PhaseExecuted Phase = "executed"
	// PhaseCommitted: the key, side effects, and cached response are committed,
	// but the response has not been sent to the client yet.
	PhaseCommitted Phase = "committed"
)

// config holds the settings shared by the in-memory and Postgres middlewares.
type config struct {
	guarded      map[string]bool
	maxBodyBytes int64
	requireKey   bool
	timeout      time.Duration
	onPhase      func(Phase)
}

// Option configures a middleware.
type Option func(*config)

// GuardMethods sets which HTTP methods are subject to idempotency handling.
// Methods are matched case-insensitively. The default set is POST, PUT, PATCH,
// DELETE; safe methods (GET, HEAD, OPTIONS) always pass straight through.
func GuardMethods(methods ...string) Option {
	return func(c *config) {
		c.guarded = make(map[string]bool, len(methods))
		for _, m := range methods {
			c.guarded[strings.ToUpper(m)] = true
		}
	}
}

// MaxBodyBytes limits how many bytes of the request body are buffered. Requests
// whose body exceeds the limit are rejected with 413. The default is 1 MiB.
func MaxBodyBytes(n int64) Option {
	return func(c *config) { c.maxBodyBytes = n }
}

// RequireKey controls what happens when a guarded request arrives without an
// Idempotency-Key header. When true (the default), the request is rejected with
// 400. When false, it is passed through unguarded.
func RequireKey(required bool) Option {
	return func(c *config) { c.requireKey = required }
}

// ExecutionTimeout bounds how long a Postgres-backed leader may hold its
// transaction open. The default is 30s. It has no effect on the in-memory
// middleware.
func ExecutionTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// OnPhase registers fn to be called as a Postgres-backed leader passes each
// Phase. It has no effect on the in-memory middleware.
func OnPhase(fn func(Phase)) Option {
	return func(c *config) { c.onPhase = fn }
}

func newConfig(opts []Option) config {
	c := config{
		guarded:      map[string]bool{http.MethodPost: true, http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true},
		maxBodyBytes: defaultMaxBodyBytes,
		requireKey:   true,
		timeout:      defaultExecutionTimeout,
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

func (c *config) phase(p Phase) {
	if c.onPhase != nil {
		c.onPhase(p)
	}
}

// admit decides whether r needs idempotency handling. For a guarded request with
// a key it buffers the body back onto r and returns the key and the request's
// fingerprint with ok == true. Otherwise it fully handles the request itself —
// passing it straight to next or rejecting it — and returns ok == false.
func (c *config) admit(w http.ResponseWriter, r *http.Request, next http.Handler) (key, hash string, ok bool) {
	if !c.guarded[r.Method] {
		next.ServeHTTP(w, r)
		return "", "", false
	}

	key = strings.TrimSpace(r.Header.Get(HeaderName))
	if key == "" {
		if c.requireKey {
			http.Error(w, HeaderName+" header is required for "+r.Method+" requests", http.StatusBadRequest)
		} else {
			next.ServeHTTP(w, r)
		}
		return "", "", false
	}

	body, err := readAtMost(r.Body, c.maxBodyBytes)
	if errors.Is(err, errBodyTooLarge) {
		http.Error(w, "request body exceeds the limit accepted by the idempotency layer", http.StatusRequestEntityTooLarge)
		return "", "", false
	} else if err != nil {
		http.Error(w, "could not read request body", http.StatusBadRequest)
		return "", "", false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	return key, fingerprint(r.Method, r.URL.Path, key, body), true
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
