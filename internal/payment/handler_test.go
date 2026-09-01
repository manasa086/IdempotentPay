package payment_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manasa086/IdempotentPay/internal/idempotency"
	"github.com/manasa086/IdempotentPay/internal/payment"
)

// newAPI wires the payment routes behind the idempotency middleware, exactly as
// cmd/server does, and returns the handler plus the ledger so tests can inspect
// side effects. Requests are served in-process via httptest.NewRecorder — no TCP
// listener — so the concurrency tests aren't bottlenecked on socket limits.
func newAPI(t *testing.T) (http.Handler, *payment.Ledger) {
	t.Helper()
	store := idempotency.NewStore(0)
	t.Cleanup(store.Close)

	ledger := payment.NewLedger()
	mux := http.NewServeMux()
	payment.NewHandler(ledger).Routes(mux)

	return idempotency.New(store).Handler(mux), ledger
}

func postCharge(h http.Handler, key, body string) (int, string) {
	req := httptest.NewRequest(http.MethodPost, "/v1/charges", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(idempotency.HeaderName, key)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code, rr.Body.String()
}

func TestChargeIsProcessedExactlyOnceUnderConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	api, ledger := newAPI(t)
	const body = `{"account":"acct_42","amount_cents":1299,"currency":"usd"}`

	const n = 200
	var wg sync.WaitGroup
	results := make([]string, n)
	statuses := make([]int, n)
	ready := make(chan struct{})

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-ready // release all goroutines at once to maximise overlap
			statuses[i], results[i] = postCharge(api, "charge-key-1", body)
		}(i)
	}
	close(ready)
	wg.Wait()

	if got := ledger.Processed(); got != 1 {
		t.Fatalf("ledger recorded %d charges, want exactly 1", got)
	}

	var firstID string
	for i := 0; i < n; i++ {
		if statuses[i] != http.StatusCreated {
			t.Fatalf("request %d: status %d, want %d (body=%s)", i, statuses[i], http.StatusCreated, results[i])
		}
		var c payment.Charge
		if err := json.Unmarshal([]byte(results[i]), &c); err != nil {
			t.Fatalf("request %d: bad JSON %q: %v", i, results[i], err)
		}
		if firstID == "" {
			firstID = c.ID
		} else if c.ID != firstID {
			t.Fatalf("request %d returned charge id %q, want %q — duplicates must replay the same charge", i, c.ID, firstID)
		}
	}

	if _, ok := ledger.Get(firstID); !ok {
		t.Fatalf("charge %q missing from ledger", firstID)
	}
}

func TestDistinctKeysProduceDistinctCharges(t *testing.T) {
	t.Parallel()
	api, ledger := newAPI(t)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"account":"acct_%d","amount_cents":100,"currency":"eur"}`, i)
			if code, b := postCharge(api, fmt.Sprintf("key-%d", i), body); code != http.StatusCreated {
				t.Errorf("key-%d: status %d body %s", i, code, b)
			}
		}(i)
	}
	wg.Wait()

	if got := ledger.Processed(); got != n {
		t.Fatalf("ledger recorded %d charges, want %d", got, n)
	}
}

func TestReplayedChargeSurvivesTiming(t *testing.T) {
	t.Parallel()
	api, ledger := newAPI(t)
	const body = `{"account":"acct_1","amount_cents":700,"currency":"gbp"}`

	code1, b1 := postCharge(api, "k", body)
	time.Sleep(5 * time.Millisecond)
	code2, b2 := postCharge(api, "k", body)

	if code1 != http.StatusCreated || code2 != http.StatusCreated {
		t.Fatalf("statuses: %d, %d", code1, code2)
	}
	if b1 != b2 {
		t.Fatalf("replay returned a different body:\n first=%s\nsecond=%s", b1, b2)
	}
	if ledger.Processed() != 1 {
		t.Fatalf("ledger recorded %d charges, want 1", ledger.Processed())
	}
}

func TestInvalidChargeIsRejectedAndNotRecorded(t *testing.T) {
	t.Parallel()
	api, ledger := newAPI(t)

	code, _ := postCharge(api, "k", `{"account":"","amount_cents":-5,"currency":"x"}`)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d, want %d", code, http.StatusUnprocessableEntity)
	}
	if ledger.Processed() != 0 {
		t.Fatalf("invalid charge was recorded")
	}
}

func TestGetChargeIsNotGuarded(t *testing.T) {
	t.Parallel()
	api, _ := newAPI(t)

	code, body := postCharge(api, "k", `{"account":"acct_1","amount_cents":250,"currency":"usd"}`)
	if code != http.StatusCreated {
		t.Fatalf("create: status %d", code)
	}
	var c payment.Charge
	_ = json.Unmarshal([]byte(body), &c)

	req := httptest.NewRequest(http.MethodGet, "/v1/charges/"+c.ID, nil)
	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET charge: status %d, want 200", rr.Code)
	}
}
