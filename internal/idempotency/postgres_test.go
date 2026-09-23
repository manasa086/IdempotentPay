package idempotency

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manasa086/IdempotentPay/internal/db"
	"github.com/manasa086/IdempotentPay/internal/db/dbtest"
)

// chargeHandler writes one row to the charges table per call, inside whatever
// transaction the middleware put in the request context. after runs once the
// row is written and may take over the response (to fail or panic).
type chargeHandler struct {
	pool  *pgxpool.Pool
	calls atomic.Int64
	delay time.Duration
	after func(call int64, w http.ResponseWriter) (handled bool)
}

func (h *chargeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := h.calls.Add(1)
	var b [8]byte
	_, _ = rand.Read(b[:])
	id := "ch_" + hex.EncodeToString(b[:])

	if _, err := db.Conn(r.Context(), h.pool).Exec(r.Context(),
		`INSERT INTO charges (id, account, amount_cents, currency) VALUES ($1, 'acct_1', 100, 'usd')`, id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	if h.after != nil && h.after(n, w) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"id":%q}`, id)
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func charges(t *testing.T, pool *pgxpool.Pool) int64 {
	return countRows(t, pool, `SELECT count(*) FROM charges`)
}

func keyRows(t *testing.T, pool *pgxpool.Pool, key string) int64 {
	return countRows(t, pool, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key)
}

// TestPostgresDuplicatesAcrossInstancesExecuteExactlyOnce runs several
// independent middleware instances — each with its own connection pool and its
// own in-process coalescing, like separate servers behind a load balancer — and
// fires duplicates at all of them at once. Only PostgreSQL's row lock on the key
// stands between them, and it must still produce exactly one charge.
func TestPostgresDuplicatesAcrossInstancesExecuteExactlyOnce(t *testing.T) {
	t.Parallel()
	s := dbtest.NewSchema(t)

	const instances, perInstance = 4, 50
	var handlers []*chargeHandler
	var servers []http.Handler
	for i := 0; i < instances; i++ {
		pool, err := db.Open(context.Background(), s.URL)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		h := &chargeHandler{pool: pool, delay: 20 * time.Millisecond}
		handlers = append(handlers, h)
		servers = append(servers, NewPostgres(pool).Handler(h))
	}

	var wg sync.WaitGroup
	bodies := make([]string, instances*perInstance)
	ready := make(chan struct{})
	for i := range bodies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-ready
			rr := doPost(servers[i%instances], "shared-key", `{"amount_cents":100}`)
			if rr.Code != http.StatusCreated {
				t.Errorf("request %d: status %d body %q", i, rr.Code, rr.Body)
			}
			bodies[i] = rr.Body.String()
		}(i)
	}
	close(ready)
	wg.Wait()

	if got := charges(t, s.Pool); got != 1 {
		t.Fatalf("recorded %d charges, want exactly 1", got)
	}
	var calls int64
	for _, h := range handlers {
		calls += h.calls.Load()
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times across %d instances, want 1", calls, instances)
	}
	for i, b := range bodies {
		if b != bodies[0] {
			t.Fatalf("request %d got %q, want %q", i, b, bodies[0])
		}
	}
}

// TestPostgresServerErrorRollsBackSideEffect shows the key and the charge share
// one transaction: a handler that writes a charge and then fails leaves neither
// behind, so the retry charges exactly once.
func TestPostgresServerErrorRollsBackSideEffect(t *testing.T) {
	t.Parallel()
	s := dbtest.NewSchema(t)
	h := &chargeHandler{pool: s.Pool, after: func(call int64, w http.ResponseWriter) bool {
		if call == 1 {
			http.Error(w, "processor timeout", http.StatusInternalServerError)
			return true
		}
		return false
	}}
	var phases []Phase
	srv := NewPostgres(s.Pool, OnPhase(func(p Phase) { phases = append(phases, p) })).Handler(h)

	if rr := doPost(srv, "k", `{}`); rr.Code != http.StatusInternalServerError {
		t.Fatalf("first call: status %d, want 500", rr.Code)
	}
	if got := charges(t, s.Pool); got != 0 {
		t.Fatalf("charge survived a failed request: %d rows", got)
	}
	if got := keyRows(t, s.Pool, "k"); got != 0 {
		t.Fatalf("key survived a failed request")
	}
	if fmt.Sprint(phases) != "[executed]" {
		t.Fatalf("phases after failure = %v, want [executed]", phases)
	}

	if rr := doPost(srv, "k", `{}`); rr.Code != http.StatusCreated {
		t.Fatalf("retry: status %d, want 201", rr.Code)
	}
	if got := charges(t, s.Pool); got != 1 {
		t.Fatalf("recorded %d charges after retry, want 1", got)
	}
	if fmt.Sprint(phases) != "[executed executed committed]" {
		t.Fatalf("phases after retry = %v", phases)
	}
}

func TestPostgresPanicRollsBackAndWakesFollowers(t *testing.T) {
	t.Parallel()
	s := dbtest.NewSchema(t)
	release := make(chan struct{})
	h := &chargeHandler{pool: s.Pool, after: func(call int64, w http.ResponseWriter) bool {
		if call == 1 {
			<-release // hold the leader until followers are queued
			panic("leader blew up")
		}
		return false
	}}
	mw := NewPostgres(s.Pool).Handler(h)
	srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = recover() }()
		mw.ServeHTTP(w, r)
	})

	leaderDone := make(chan struct{})
	go func() {
		doPost(srv, "k", `{}`)
		close(leaderDone)
	}()
	time.Sleep(20 * time.Millisecond)

	const followers = 20
	codes := make([]int, followers)
	var wg sync.WaitGroup
	wg.Add(followers)
	for i := 0; i < followers; i++ {
		go func(i int) {
			defer wg.Done()
			codes[i] = doPost(srv, "k", `{}`).Code
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	<-leaderDone

	for i, c := range codes {
		if c != http.StatusBadGateway {
			t.Fatalf("follower %d: status %d, want 502", i, c)
		}
	}
	if got := charges(t, s.Pool); got != 0 {
		t.Fatalf("charge survived a panic: %d rows", got)
	}
	if rr := doPost(srv, "k", `{}`); rr.Code != http.StatusCreated {
		t.Fatalf("retry after panic: status %d, want 201", rr.Code)
	}
	if got := charges(t, s.Pool); got != 1 {
		t.Fatalf("recorded %d charges after retry, want 1", got)
	}
}

// TestPostgresClientDisconnectStillCommits checks that a leader finishes even if
// its client goes away, so the client's retry replays instead of re-charging.
func TestPostgresClientDisconnectStillCommits(t *testing.T) {
	t.Parallel()
	s := dbtest.NewSchema(t)
	ctx, disconnect := context.WithCancel(context.Background())
	h := &chargeHandler{pool: s.Pool, after: func(call int64, w http.ResponseWriter) bool {
		if call == 1 {
			disconnect()
		}
		return false
	}}
	srv := NewPostgres(s.Pool).Handler(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/charges", strings.NewReader(`{}`)).WithContext(ctx)
	req.Header.Set(HeaderName, "k")
	srv.ServeHTTP(httptest.NewRecorder(), req)

	if got := charges(t, s.Pool); got != 1 {
		t.Fatalf("recorded %d charges, want 1", got)
	}
	retry := doPost(srv, "k", `{}`)
	if retry.Code != http.StatusCreated || retry.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry: status %d replayed=%q, want 201 replayed", retry.Code, retry.Header().Get("Idempotent-Replayed"))
	}
	if h.calls.Load() != 1 || charges(t, s.Pool) != 1 {
		t.Fatalf("handler ran %d times, %d charges; want 1 and 1", h.calls.Load(), charges(t, s.Pool))
	}
}

func TestPostgresReplayAcrossInstances(t *testing.T) {
	t.Parallel()
	s := dbtest.NewSchema(t)
	a := &chargeHandler{pool: s.Pool}
	b := &chargeHandler{pool: s.Pool}

	first := doPost(NewPostgres(s.Pool).Handler(a), "k", `{"amount_cents":100}`)
	second := doPost(NewPostgres(s.Pool).Handler(b), "k", `{"amount_cents":100}`)

	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("statuses %d, %d", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("second instance returned %q, want replay of %q", second.Body, first.Body)
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("second instance's response should be marked replayed")
	}
	if second.Header().Get("Content-Type") != "application/json" {
		t.Errorf("replayed Content-Type = %q, want stored header", second.Header().Get("Content-Type"))
	}
	if b.calls.Load() != 0 || charges(t, s.Pool) != 1 {
		t.Fatalf("second instance re-executed: calls=%d charges=%d", b.calls.Load(), charges(t, s.Pool))
	}
}

func TestPostgresSameKeyDifferentPayloadIsConflict(t *testing.T) {
	t.Parallel()
	s := dbtest.NewSchema(t)
	h := &chargeHandler{pool: s.Pool}
	srv := NewPostgres(s.Pool).Handler(h)

	if rr := doPost(srv, "k", `{"amount_cents":100}`); rr.Code != http.StatusCreated {
		t.Fatalf("first: status %d", rr.Code)
	}
	rr := doPost(srv, "k", `{"amount_cents":999}`)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key: status %d, want 422", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if !strings.Contains(string(body), "different request") {
		t.Errorf("conflict body = %q", body)
	}
	if charges(t, s.Pool) != 1 {
		t.Fatalf("conflicting request created a charge")
	}
}

func TestPostgresClientErrorIsStored(t *testing.T) {
	t.Parallel()
	s := dbtest.NewSchema(t)
	var calls atomic.Int64
	srv := NewPostgres(s.Pool).Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "invalid amount", http.StatusUnprocessableEntity)
	}))

	first := doPost(srv, "k", `{}`)
	second := doPost(srv, "k", `{}`)
	if first.Code != http.StatusUnprocessableEntity || second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("statuses %d, %d; want 422 twice", first.Code, second.Code)
	}
	if calls.Load() != 1 {
		t.Fatalf("a definitive 4xx should be stored and replayed; handler ran %d times", calls.Load())
	}
}
