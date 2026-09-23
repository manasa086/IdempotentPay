# IdempotentPay

A concurrency-safe, crash-safe **idempotency layer** for REST APIs that move
money, written in Go and backed by PostgreSQL.

For any request carrying an `Idempotency-Key` header it guarantees:

1. **Exactly-once side effects, even across crashes.** The key, the charge, and
   the stored response are committed in **one PostgreSQL transaction**. A crash
   before commit leaves nothing behind; a crash after commit leaves a stored
   response that the client's retry replays. Either way the customer is charged
   once.
2. **Deduplication.** Once a key has been processed, later requests with the
   same key replay the stored response instead of re-running the handler.
3. **Request coalescing.** While the first ("leader") request for a key is in
   flight, concurrent duplicates wait for that single execution instead of
   racing into the handler: in-process on a shared channel, and across server
   instances on PostgreSQL's row lock for the key.

## Why it exists

Payment clients retry aggressively: on timeouts, connection resets, and 5xx
responses. Without an idempotency layer, three retries of "charge $12.99" become
three charges. A simple "look up the key, then charge" check has two gaps:

- **The in-flight race.** Two retries arriving while the first is still running
  both see "no key yet" and both charge.
- **The crash window.** If the server charges, then crashes before recording the
  key (or before replying), the retry charges again. The client can't tell
  "crashed before charging" from "crashed after charging", so it has to retry.

IdempotentPay closes the first gap with request coalescing and the second by
making the key and the charge a single atomic write.

## How a request flows (PostgreSQL store)

```
POST /v1/charges   Idempotency-Key: abc
        │
        ▼
 ┌─ in-process flight for "abc"? ──── yes ──► wait on leader's channel ──► replay its result
 │      no (this request is the leader)
 ▼
BEGIN
  INSERT INTO idempotency_keys (key, request_hash) ... ON CONFLICT DO NOTHING
     │   (if another instance holds an uncommitted row for "abc", this blocks
     │    until that transaction commits or rolls back)
     ├── 0 rows: key already committed ──► SELECT stored response ──► replay it
     │                                      (different payload ──► 422)
     └── 1 row: we own the key
          run the handler with the transaction in its context
            └── INSERT INTO charges ...            ◄── same transaction
          handler returned 5xx or panicked ──► ROLLBACK (charge and key both vanish)
          UPDATE idempotency_keys SET response_status, response_header, response_body
COMMIT                                             ◄── key + charge + response, atomically
        │
        ▼
write the response to the client
```

| Crash point | What PostgreSQL holds | What the client's retry gets |
| --- | --- | --- |
| Before `COMMIT` | nothing (transaction rolled back when the connection dropped) | a fresh execution, exactly one charge |
| After `COMMIT`, before the reply | key + charge + stored response | a replay of the original response, no new charge |

Both rows of this table are verified by tests that kill a real server process
at that point (see [Tests](#tests)).

## Design

| Concern | Approach |
| --- | --- |
| Atomicity | `PostgresMiddleware` opens the transaction and passes it to the handler through the request context (`db.WithTx`). The ledger writes with `db.Conn(ctx, pool)`, which joins that transaction. |
| Cross-instance coalescing | The leader's `INSERT` of the key holds the primary-key lock until it commits. A duplicate `INSERT` on another instance blocks on it, then either sees the committed row (replay) or, after a rollback, becomes the new leader. |
| In-process coalescing | A `flightGroup` (map + mutex + per-key `done` channel) makes duplicates in the same process wait on the leader without each holding a database connection. Closing the channel publishes the result with a happens-before edge, so no lock is needed to read it. |
| Key reuse with a different payload | Requests are fingerprinted (`sha256` of method + path + key + body). A mismatch returns **422**. |
| Server errors and panics | Rolled back, not stored: the side effect is undone with the key, so a retry re-executes safely. Waiting duplicates get a retryable **502**. Definitive 4xx responses are stored and replayed. |
| Client disconnect | A leader runs on a context detached from its client (`context.WithoutCancel`, bounded by a 30s timeout), so a disconnect doesn't abort work that duplicates are waiting on. The client's retry replays the committed result. |
| Scope | The guarantee covers side effects written to PostgreSQL inside the transaction. A call to an external processor would also need the key passed downstream. The transaction is held open while the handler runs, which suits short database-bound handlers. |

An **in-memory store** (`idempotency.Middleware`) with the same deduplication
and coalescing is included for running without a database. It is single-process
and doesn't survive restarts; it expires keys with a TTL.

### Layout

```
cmd/server/              demo payment API; -store=memory|postgres; crash failpoint for tests
internal/idempotency/
  postgres.go            PostgresMiddleware: transactional key reservation, replay, rollback policy
  flight.go              in-process coalescing for the Postgres middleware
  middleware.go          in-memory Middleware
  store.go               in-memory mutex-guarded store with leader election and TTL
  config.go              options, request fingerprinting, body limits
  recorder.go            buffering ResponseWriter and response replay
internal/payment/        POST /v1/charges, GET /v1/charges/{id}; memory and Postgres ledgers
internal/db/             pool, schema, transaction-in-context helpers; dbtest gives each test its own schema
```

## Run it

With PostgreSQL:

```bash
createdb idempotentpay
go run ./cmd/server -store=postgres -database-url "postgres://localhost:5432/idempotentpay?sslmode=disable"
```

The server creates its tables on startup. Or run without a database:

```bash
go run ./cmd/server            # -store=memory, listens on :8080
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

Flags: `-addr` (default `:8080`), `-store` (`memory` or `postgres`),
`-database-url` (default `$DATABASE_URL`), `-idempotency-ttl` (memory store only).

## Using the layer in your own service

```go
pool, _ := db.Open(ctx, databaseURL)
_ = db.Migrate(ctx, pool)

mux := http.NewServeMux()
mux.HandleFunc("POST /v1/charges", func(w http.ResponseWriter, r *http.Request) {
    // Joins the idempotency transaction, so this commits with the key.
    db.Conn(r.Context(), pool).Exec(r.Context(), `INSERT INTO charges ...`)
    // ...
})

http.ListenAndServe(":8080", idempotency.NewPostgres(pool).Handler(mux))
```

## Tests

The guarantees are the point, so they're tested directly, under Go's race
detector, against a real PostgreSQL server:

| Test | Asserts |
| --- | --- |
| `TestCrashBeforeCommitLeavesNoCharge` | Starts the real server binary, kills it after the charge is written but before commit, restarts it and retries → **0** charges after the crash, exactly **1** after the retry |
| `TestCrashAfterCommitReplaysOnRetry` | Kills the server after commit but before it replies, restarts and retries → the retry **replays** the original charge, still exactly **1** |
| `TestChargeIsProcessedExactlyOnceUnderConcurrentDuplicates` | 250 simultaneous `POST /v1/charges` with one key, against both the Postgres and in-memory stores → exactly **1** charge, every response carries the same charge id |
| `TestPostgresDuplicatesAcrossInstancesExecuteExactlyOnce` | 4 independent middleware instances (separate pools, like separate servers) × 50 duplicates → handler runs **once** in total |
| `TestPostgresServerErrorRollsBackSideEffect` | handler writes a charge then returns 500 → the charge and the key are both rolled back; the retry charges once |
| `TestPostgresPanicRollsBackAndWakesFollowers` | leader panics mid-charge → rollback, waiting duplicates get 502, the retry charges once |
| `TestPostgresClientDisconnectStillCommits` | the client disconnects mid-request → the charge still commits; the retry replays it |
| `TestPostgresReplayAcrossInstances` | a key committed by one instance is replayed by another, stored headers included |
| `TestStoreBeginIsSingleLeaderUnderRace` | 500 goroutines race to claim one key in the in-memory store → exactly **1** leader |

To confirm the tests catch real bugs, they were also run against deliberately
broken code. Skipping the replay produced 4 charges across 4 instances. Writing
the charge outside the transaction left a charge behind after the crash. Both
were caught.

```bash
createdb idempotentpay_test
make race     # go test -race -count=1 ./...  (TEST_DATABASE_URL defaults to the local idempotentpay_test database)
make cover
```

Database tests are skipped when `TEST_DATABASE_URL` is unset. CI runs them
against a PostgreSQL 16 service container.
