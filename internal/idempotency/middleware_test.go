package idempotency

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingHandler records how many times it was actually invoked and, on each
// call, sleeps briefly so concurrent requests genuinely overlap.
type countingHandler struct {
	calls   atomic.Int64
	delay   time.Duration
	status  int
	bodyFmt string // formatted with the call number
}

func (h *countingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := h.calls.Add(1)
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	if h.status != 0 {
		w.WriteHeader(h.status)
	}
	fmt.Fprintf(w, h.bodyFmt, n)
}

func newTestMiddleware(t *testing.T, next http.Handler, opts ...Option) http.Handler {
	t.Helper()
	store := NewStore(0)
	t.Cleanup(store.Close)
	return New(store, opts...).Handler(next)
}

func doPost(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/charges", strings.NewReader(body))
	if key != "" {
		req.Header.Set(HeaderName, key)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestConcurrentDuplicatesExecuteExactlyOnce(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{delay: 25 * time.Millisecond, status: http.StatusCreated, bodyFmt: `{"call":%d}`}
	h := newTestMiddleware(t, handler)

	const n = 250
	var wg sync.WaitGroup
	bodies := make([]string, n)
	codes := make([]int, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			rr := doPost(h, "key-burst", `{"account":"acct_1","amount_cents":500,"currency":"usd"}`)
			codes[i] = rr.Code
			bodies[i] = rr.Body.String()
		}(i)
	}
	wg.Wait()

	if got := handler.calls.Load(); got != 1 {
		t.Fatalf("handler executed %d times, want exactly 1", got)
	}
	for i := 0; i < n; i++ {
		if codes[i] != http.StatusCreated {
			t.Fatalf("request %d: status %d, want %d", i, codes[i], http.StatusCreated)
		}
		if bodies[i] != `{"call":1}` {
			t.Fatalf("request %d: body %q, want %q", i, bodies[i], `{"call":1}`)
		}
	}
}

func TestConcurrentDistinctKeysEachExecuteOnce(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{delay: 5 * time.Millisecond, status: http.StatusCreated, bodyFmt: `{"call":%d}`}
	h := newTestMiddleware(t, handler)

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			rr := doPost(h, fmt.Sprintf("key-%d", i), `{"account":"acct_1","amount_cents":500,"currency":"usd"}`)
			if rr.Code != http.StatusCreated {
				t.Errorf("key-%d: status %d, want %d", i, rr.Code, http.StatusCreated)
			}
		}(i)
	}
	wg.Wait()

	if got := handler.calls.Load(); got != n {
		t.Fatalf("handler executed %d times, want %d", got, n)
	}
}

func TestSequentialDuplicateReplaysCachedResponse(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{status: http.StatusCreated, bodyFmt: `{"call":%d}`}
	h := newTestMiddleware(t, handler)

	first := doPost(h, "key-1", `{"amount_cents":100}`)
	second := doPost(h, "key-1", `{"amount_cents":100}`)

	if handler.calls.Load() != 1 {
		t.Fatalf("handler executed %d times, want 1", handler.calls.Load())
	}
	if first.Body.String() != second.Body.String() || first.Code != second.Code {
		t.Fatalf("replay mismatch: first=(%d,%q) second=(%d,%q)", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if first.Header().Get("Idempotent-Replayed") != "" {
		t.Errorf("first response should not be marked replayed")
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("second response should carry Idempotent-Replayed: true")
	}
}

func TestSameKeyDifferentPayloadIsConflict(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{status: http.StatusCreated, bodyFmt: `{"call":%d}`}
	h := newTestMiddleware(t, handler)

	if rr := doPost(h, "key-1", `{"amount_cents":100}`); rr.Code != http.StatusCreated {
		t.Fatalf("first request: status %d, want %d", rr.Code, http.StatusCreated)
	}
	rr := doPost(h, "key-1", `{"amount_cents":999}`)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key with new payload: status %d, want %d", rr.Code, http.StatusUnprocessableEntity)
	}
	if handler.calls.Load() != 1 {
		t.Fatalf("handler executed %d times, want 1", handler.calls.Load())
	}
}

func TestMissingKeyRejectedByDefault(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{bodyFmt: "ok"}
	h := newTestMiddleware(t, handler)

	rr := doPost(h, "", `{}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing key: status %d, want %d", rr.Code, http.StatusBadRequest)
	}
	if handler.calls.Load() != 0 {
		t.Fatalf("handler ran despite missing key")
	}
}

func TestMissingKeyPassesThroughWhenNotRequired(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{status: http.StatusCreated, bodyFmt: "ok"}
	h := newTestMiddleware(t, handler, RequireKey(false))

	rr := doPost(h, "", `{}`)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status %d, want %d", rr.Code, http.StatusCreated)
	}
	if handler.calls.Load() != 1 {
		t.Fatalf("handler executed %d times, want 1", handler.calls.Load())
	}
}

func TestSafeMethodsBypassLayer(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{bodyFmt: "ok"}
	h := newTestMiddleware(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/charges/ch_1", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK || handler.calls.Load() != 1 {
		t.Fatalf("GET should pass straight through: status=%d calls=%d", rr.Code, handler.calls.Load())
	}
}

func TestServerErrorIsNotCached(t *testing.T) {
	t.Parallel()
	var call atomic.Int64
	flaky := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if call.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, "boom")
			return
		}
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "ok")
	})
	h := newTestMiddleware(t, flaky)

	if rr := doPost(h, "key-1", `{}`); rr.Code != http.StatusInternalServerError {
		t.Fatalf("first call: status %d, want 500", rr.Code)
	}
	rr := doPost(h, "key-1", `{}`)
	if rr.Code != http.StatusCreated || rr.Body.String() != "ok" {
		t.Fatalf("retry after 500 should re-execute: status=%d body=%q", rr.Code, rr.Body.String())
	}
	if call.Load() != 2 {
		t.Fatalf("handler executed %d times, want 2 (500 not memoized)", call.Load())
	}
}

func TestPanicReleasesKeyAndWakesFollowers(t *testing.T) {
	t.Parallel()
	var call atomic.Int64
	start := make(chan struct{})
	h := newTestMiddleware(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := call.Add(1)
		if n == 1 {
			<-start // hold the leader open until followers are queued
			panic("leader blew up")
		}
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "recovered")
	}))
	// Wrap with a recover so the panicking leader goroutine doesn't fail the test.
	safe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = recover() }()
		h.ServeHTTP(w, r)
	})

	const followers = 20
	var wg sync.WaitGroup
	codes := make([]int, followers)

	leaderDone := make(chan struct{})
	go func() {
		doPost(safe, "key-1", `{}`)
		close(leaderDone)
	}()
	time.Sleep(10 * time.Millisecond) // let the leader register

	wg.Add(followers)
	for i := 0; i < followers; i++ {
		go func(i int) {
			defer wg.Done()
			codes[i] = doPost(safe, "key-1", `{}`).Code
		}(i)
	}
	time.Sleep(10 * time.Millisecond)
	close(start)
	wg.Wait()
	<-leaderDone

	for i, c := range codes {
		if c != http.StatusBadGateway {
			t.Fatalf("follower %d: status %d, want %d (leader panicked)", i, c, http.StatusBadGateway)
		}
	}
	// Key was released, so a fresh request now succeeds.
	if rr := doPost(safe, "key-1", `{}`); rr.Code != http.StatusCreated {
		t.Fatalf("post-panic retry: status %d, want %d", rr.Code, http.StatusCreated)
	}
}

func TestOversizedBodyRejected(t *testing.T) {
	t.Parallel()
	handler := &countingHandler{bodyFmt: "ok"}
	store := NewStore(0)
	t.Cleanup(store.Close)
	h := New(store, MaxBodyBytes(16)).Handler(handler)

	rr := doPost(h, "key-1", strings.Repeat("x", 64))
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want %d", rr.Code, http.StatusRequestEntityTooLarge)
	}
	if handler.calls.Load() != 0 {
		t.Fatalf("handler ran on oversized body")
	}
}
