// Command server runs the IdempotentPay demo: a payment REST API whose
// mutating endpoints are protected by the idempotency layer in
// github.com/manasa086/IdempotentPay/internal/idempotency.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/manasa086/IdempotentPay/internal/idempotency"
	"github.com/manasa086/IdempotentPay/internal/payment"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	ttl := flag.Duration("idempotency-ttl", 24*time.Hour, "how long to retain idempotency keys (0 = forever)")
	flag.Parse()

	store := idempotency.NewStore(*ttl)
	defer store.Close()

	ledger := payment.NewLedger()
	api := payment.NewHandler(ledger)

	mux := http.NewServeMux()
	api.Routes(mux)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})

	idem := idempotency.New(store)
	handler := requestLog(idem.Handler(mux))

	srv := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}

// requestLog is a tiny logging middleware so the demo prints what it served,
// including whether a response was replayed from the idempotency cache.
func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		log.Printf("%s %s -> %d (%s) replayed=%s",
			r.Method, r.URL.Path, sw.status, time.Since(start).Round(time.Microsecond),
			replayed(sw.Header().Get("Idempotent-Replayed")))
	})
}

func replayed(v string) string {
	if v == "true" {
		return "true"
	}
	return "false"
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
