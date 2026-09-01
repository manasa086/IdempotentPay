# IdempotentPay

A concurrency-safe **idempotency layer** for REST APIs that perform side effects
(charges, transfers, order creation), written in Go with the standard library
only.

It gives two guarantees for any request carrying an `Idempotency-Key` header:

1. **Deduplication** — once a key has been processed, later requests with the
   same key replay the stored response instead of re-running the handler.
2. **Request coalescing** — while the first ("leader") request for a key is
   still in flight, concurrent duplicates block on that single execution rather
   than racing into the handler independently. The side effect therefore runs
   **exactly once per key**, even under a burst of simultaneous retries.

```
                       ┌─────────────────────────── Idempotency-Key: "abc" ───┐
 client (retry storm)  │  req₁  req₂  req₃ ... req₂₀₀                          │
                       └───┬─────┬─────┬──────────┬──────────────────────────-─┘
                           ▼     ▼     ▼          ▼
                     ┌──────────────────────────────────┐
                     │  idempotency.Middleware          │
                     │  ── begin("abc", fingerprint) ── │
                     │   leader → runs handler once     │      followers block
                     │   followers → wait on <-done ────┼────► on the leader's
                     │   done → replay cached response  │      single execution
                     └───────────────┬──────────────────┘
                                     ▼  (exactly one call)
                            payment handler → ledger  (1 charge recorded)
```

## Why it exists

Payment clients retry aggressively — on timeout, on connection reset, on a 5xx.
Without an idempotency layer, three retries of "charge $12.99" can become three
charges. The usual fix (dedupe on a stored key) still has a race: if two retries
arrive *while the first is mid-flight*, a naive check-then-insert lets both
through. IdempotentPay closes that window by making duplicates **wait on the
in-flight execution** and share its result.

## Design

| Concern | Approach |
| --- | --- |
| Shared state | Single in-memory map guarded by one `sync.Mutex` (`internal/idempotency/store.go`). |
| Coalescing | Each key gets an `entry` with a `done` channel. The leader closes it after executing; followers `select` on `<-entry.done` (or their own request context). Closing the channel publishes the cached result with a happens-before edge — no lock needed on the read path. |
| Leader election | `store.begin(key, hash)` returns exactly one `outcomeLeader` per key under concurrent callers; everyone else is `outcomeFollower` or `outcomeConflict`. |
| Key reuse with a different payload | Request is fingerprinted (`sha256` of method + path + key + body). A mismatch returns **422 Unprocessable Entity**. |
| Transient failures | A `5xx` response or a handler **panic** releases the key (`store.abort`) instead of caching it, so retries can re-execute. In-flight followers get a retryable **502**. |
| Client disconnect | If the leader's request context is cancelled mid-flight, the partial response is not cached. |
| Memory | Optional TTL: completed entries expire `ttl` after creation, swept by a background janitor; expiry is also checked lazily in `begin` so correctness never depends on the sweep. In-flight entries are never evicted. |
| Response capture | A buffering `http.ResponseWriter` records status + headers + body. Guarded responses are not streamed incrementally — fine for the create-style endpoints idempotency keys protect. |

### What's in the box

```
cmd/server/            demo payment API wired behind the middleware
internal/idempotency/   the reusable layer
  store.go              mutex-guarded entry store, leader election, TTL
  middleware.go         net/http middleware: fingerprinting, coalescing, error policy
  recorder.go           buffering ResponseWriter + cached-response replay
internal/payment/       side-effecting handler used to prove exactly-once
  ledger.go             in-memory charge ledger with a Processed() counter
  handler.go            POST /v1/charges, GET /v1/charges/{id}
```

## Run it

```bash
go run ./cmd/server            # listens on :8080
# or
make run
```

```bash
# 1) first charge
curl -si -X POST localhost:8080/v1/charges \
  -H 'Idempotency-Key: abc' -H 'Content-Type: application/json' \
  -d '{"account":"acct_1","amount_cents":1299,"currency":"usd"}'
# → 201 Created, {"id":"ch_...", ...}

# 2) same key again → identical body, plus a replay marker, no new charge
curl -si -X POST localhost:8080/v1/charges \
  -H 'Idempotency-Key: abc' -H 'Content-Type: application/json' \
  -d '{"account":"acct_1","amount_cents":1299,"currency":"usd"}'
# → 201 Created, Idempotent-Replayed: true

# 3) same key, different amount → rejected
curl -s -X POST localhost:8080/v1/charges \
  -H 'Idempotency-Key: abc' -H 'Content-Type: application/json' \
  -d '{"account":"acct_1","amount_cents":9999,"currency":"usd"}'
# → 422 Unprocessable Entity

# 4) no key → rejected
curl -s -X POST localhost:8080/v1/charges -d '{}'
# → 400 Bad Request
```

Flags: `-addr` (default `:8080`), `-idempotency-ttl` (default `24h`, `0` = keep forever).

## Using the layer in your own service

```go
store := idempotency.NewStore(24 * time.Hour)
defer store.Close()

mux := http.NewServeMux()
// ... register handlers ...

idem := idempotency.New(store,
    idempotency.GuardMethods(http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete),
    idempotency.RequireKey(true),
    idempotency.MaxBodyBytes(1<<20),
)
http.ListenAndServe(":8080", idem.Handler(mux))
```

## Tests

The concurrency guarantees are the point, so they're tested directly:

| Test | Asserts |
| --- | --- |
| `TestConcurrentDuplicatesExecuteExactlyOnce` | 250 goroutines, one key, ~25ms handler → handler runs **once**, all 250 get the same 201 body |
| `TestChargeIsProcessedExactlyOnceUnderConcurrentDuplicates` | 200 concurrent `POST /v1/charges`, one key → ledger records **1** charge, every response carries the same charge id |
| `TestStoreBeginIsSingleLeaderUnderRace` | 500 goroutines call `begin` on one key → exactly **1** elected leader |
| `TestConcurrentDistinctKeysEachExecuteOnce` | 100 goroutines, 100 keys → 100 executions |
| `TestServerErrorIsNotCached` | a 500 is not memoized; the retry re-executes |
| `TestPanicReleasesKeyAndWakesFollowers` | leader panic → followers get 502, key freed, next request succeeds |
| `TestSameKeyDifferentPayloadIsConflict` | key reuse with a new body → 422 |
| `TestStoreExpiryPromotesNextCallerToLeader` | after TTL, the key is processed fresh |

```bash
make race     # go test -race -count=1 ./...
make cover    # coverage across ./internal/...
```

All tests run clean under `-race`.
