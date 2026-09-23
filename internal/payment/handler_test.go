package payment_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/manasa086/IdempotentPay/internal/db/dbtest"
	"github.com/manasa086/IdempotentPay/internal/idempotency"
	"github.com/manasa086/IdempotentPay/internal/payment"
)

// api is the payment API wired behind an idempotency middleware, exactly as
// cmd/server does, plus a way to count the charges it has recorded.
type api struct {
	http.Handler
	ledger interface {
		Count(context.Context) (int64, error)
	}
}

func (a api) charges(t *testing.T) int64 {
	t.Helper()
	n, err := a.ledger.Count(context.Background())
	if err != nil {
		t.Fatalf("count charges: %v", err)
	}
	return n
}

// stores lists every backend; each test runs against all of them. The postgres
// backend is skipped unless TEST_DATABASE_URL is set.
var stores = []struct {
	name string
	new  func(t *testing.T) api
}{
	{"memory", func(t *testing.T) api {
		keys := idempotency.NewStore(0)
		t.Cleanup(keys.Close)
		ledger := payment.NewMemoryLedger()
		mux := http.NewServeMux()
		payment.NewHandler(ledger).Routes(mux)
		return api{idempotency.New(keys).Handler(mux), ledger}
	}},
	{"postgres", func(t *testing.T) api {
		s := dbtest.NewSchema(t)
		ledger := payment.NewPostgresLedger(s.Pool)
		mux := http.NewServeMux()
		payment.NewHandler(ledger).Routes(mux)
		return api{idempotency.NewPostgres(s.Pool).Handler(mux), ledger}
	}},
}

func forEachStore(t *testing.T, test func(t *testing.T, a api)) {
	for _, s := range stores {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			test(t, s.new(t))
		})
	}
}

// Requests are served in-process via httptest.NewRecorder — no TCP listener —
// so the concurrency tests aren't bottlenecked on socket limits.
func postCharge(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/charges", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(idempotency.HeaderName, key)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestChargeIsProcessedExactlyOnceUnderConcurrentDuplicates(t *testing.T) {
	forEachStore(t, func(t *testing.T, a api) {
		const body = `{"account":"acct_42","amount_cents":1299,"currency":"usd"}`
		const n = 250

		var wg sync.WaitGroup
		responses := make([]*httptest.ResponseRecorder, n)
		ready := make(chan struct{})
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				<-ready // release all goroutines at once to maximise overlap
				responses[i] = postCharge(a, "charge-key-1", body)
			}(i)
		}
		close(ready)
		wg.Wait()

		if got := a.charges(t); got != 1 {
			t.Fatalf("recorded %d charges, want exactly 1", got)
		}

		var firstID string
		for i, rr := range responses {
			if rr.Code != http.StatusCreated {
				t.Fatalf("request %d: status %d, want %d (body=%s)", i, rr.Code, http.StatusCreated, rr.Body)
			}
			var c payment.Charge
			if err := json.Unmarshal(rr.Body.Bytes(), &c); err != nil {
				t.Fatalf("request %d: bad JSON %q: %v", i, rr.Body, err)
			}
			if firstID == "" {
				firstID = c.ID
			} else if c.ID != firstID {
				t.Fatalf("request %d returned charge %q, want %q — duplicates must replay the same charge", i, c.ID, firstID)
			}
		}
	})
}

func TestDistinctKeysProduceDistinctCharges(t *testing.T) {
	forEachStore(t, func(t *testing.T, a api) {
		const n = 50
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				body := fmt.Sprintf(`{"account":"acct_%d","amount_cents":100,"currency":"eur"}`, i)
				if rr := postCharge(a, fmt.Sprintf("key-%d", i), body); rr.Code != http.StatusCreated {
					t.Errorf("key-%d: status %d body %s", i, rr.Code, rr.Body)
				}
			}(i)
		}
		wg.Wait()

		if got := a.charges(t); got != n {
			t.Fatalf("recorded %d charges, want %d", got, n)
		}
	})
}

func TestSequentialRetryReplaysCharge(t *testing.T) {
	forEachStore(t, func(t *testing.T, a api) {
		const body = `{"account":"acct_1","amount_cents":700,"currency":"gbp"}`

		first := postCharge(a, "k", body)
		second := postCharge(a, "k", body)

		if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
			t.Fatalf("statuses: %d, %d", first.Code, second.Code)
		}
		if first.Body.String() != second.Body.String() {
			t.Fatalf("replay returned a different body:\n first=%s\nsecond=%s", first.Body, second.Body)
		}
		if second.Header().Get("Idempotent-Replayed") != "true" {
			t.Errorf("retry should carry Idempotent-Replayed: true")
		}
		if got := a.charges(t); got != 1 {
			t.Fatalf("recorded %d charges, want 1", got)
		}
	})
}

func TestInvalidChargeIsRejectedAndNotRecorded(t *testing.T) {
	forEachStore(t, func(t *testing.T, a api) {
		rr := postCharge(a, "k", `{"account":"","amount_cents":-5,"currency":"x"}`)
		if rr.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status %d, want %d", rr.Code, http.StatusUnprocessableEntity)
		}
		if got := a.charges(t); got != 0 {
			t.Fatalf("invalid charge was recorded")
		}
	})
}

func TestGetChargeIsNotGuarded(t *testing.T) {
	forEachStore(t, func(t *testing.T, a api) {
		rr := postCharge(a, "k", `{"account":"acct_1","amount_cents":250,"currency":"usd"}`)
		if rr.Code != http.StatusCreated {
			t.Fatalf("create: status %d", rr.Code)
		}
		var c payment.Charge
		_ = json.Unmarshal(rr.Body.Bytes(), &c)

		get := httptest.NewRecorder()
		a.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/charges/"+c.ID, nil))
		if get.Code != http.StatusOK {
			t.Fatalf("GET charge: status %d, want 200", get.Code)
		}
		missing := httptest.NewRecorder()
		a.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/v1/charges/ch_nope", nil))
		if missing.Code != http.StatusNotFound {
			t.Fatalf("GET missing charge: status %d, want 404", missing.Code)
		}
	})
}
