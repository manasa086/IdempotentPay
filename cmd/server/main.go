// Command server runs the IdempotentPay demo: a payment REST API whose
// mutating endpoints are protected by the idempotency layer in
// github.com/manasa086/IdempotentPay/internal/idempotency.
//
// With -store=memory (the default) everything lives in process memory. With
// -store=postgres, idempotency keys and charges are committed together in
// PostgreSQL, so a charge happens exactly once even if the server crashes
// mid-request.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/manasa086/IdempotentPay/internal/db"
	"github.com/manasa086/IdempotentPay/internal/idempotency"
	"github.com/manasa086/IdempotentPay/internal/payment"
)

// crashEnv names a failpoint for the crash-recovery tests: when set to an
// idempotency.Phase, the server exits abruptly as a leader reaches that phase.
const crashEnv = "IDEMPOTENTPAY_CRASH_AT"

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	store := flag.String("store", "memory", "where idempotency keys and charges live: memory or postgres")
	dbURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL URL for -store=postgres (default $DATABASE_URL)")
	ttl := flag.Duration("idempotency-ttl", 24*time.Hour, "how long to retain idempotency keys with -store=memory (0 = forever)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handler, cleanup, err := build(ctx, *store, *dbURL, *ttl)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{
		Handler:           requestLog(handler),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("listening on %s (store=%s)", ln.Addr(), *store)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}

// build wires the payment API behind the idempotency middleware for the chosen
// store and returns the handler plus a function that releases its resources.
func build(ctx context.Context, store, dbURL string, ttl time.Duration) (http.Handler, func(), error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})

	switch store {
	case "memory":
		keys := idempotency.NewStore(ttl)
		payment.NewHandler(payment.NewMemoryLedger()).Routes(mux)
		return idempotency.New(keys).Handler(mux), keys.Close, nil

	case "postgres":
		if dbURL == "" {
			return nil, nil, errors.New("-store=postgres needs -database-url or $DATABASE_URL")
		}
		pool, err := db.Open(ctx, dbURL)
		if err != nil {
			return nil, nil, err
		}
		if err := db.Migrate(ctx, pool); err != nil {
			pool.Close()
			return nil, nil, err
		}
		payment.NewHandler(payment.NewPostgresLedger(pool)).Routes(mux)
		return idempotency.NewPostgres(pool, crashOptions()...).Handler(mux), pool.Close, nil

	default:
		return nil, nil, fmt.Errorf("unknown -store %q (want memory or postgres)", store)
	}
}

// crashOptions installs the failpoint named by $IDEMPOTENTPAY_CRASH_AT, if any.
func crashOptions() []idempotency.Option {
	at := idempotency.Phase(os.Getenv(crashEnv))
	if at == "" {
		return nil
	}
	log.Printf("failpoint armed: will crash at phase %q", at)
	return []idempotency.Option{idempotency.OnPhase(func(p idempotency.Phase) {
		if p == at {
			log.Printf("failpoint: crashing at phase %q", p)
			os.Exit(3)
		}
	})}
}

// requestLog is a tiny logging middleware so the demo prints what it served,
// including whether a response was replayed from the idempotency cache.
func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		log.Printf("%s %s -> %d (%s) replayed=%t",
			r.Method, r.URL.Path, sw.status, time.Since(start).Round(time.Microsecond),
			sw.Header().Get("Idempotent-Replayed") == "true")
	})
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
}
