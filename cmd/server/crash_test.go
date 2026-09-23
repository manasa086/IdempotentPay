package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manasa086/IdempotentPay/internal/db/dbtest"
	"github.com/manasa086/IdempotentPay/internal/idempotency"
)

// These tests kill a real server process in the middle of a charge, restart it,
// retry the same request, and count what PostgreSQL actually committed.

const chargeBody = `{"account":"acct_crash","amount_cents":4200,"currency":"usd"}`

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
	binDir    string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if binDir != "" {
		os.RemoveAll(binDir)
	}
	os.Exit(code)
}

func serverBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "idempotentpay-bin")
		if buildErr != nil {
			return
		}
		binPath = filepath.Join(binDir, "server")
		out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput()
		if err != nil {
			buildErr = errors.New(string(out))
		}
	})
	if buildErr != nil {
		t.Fatalf("build server: %v", buildErr)
	}
	return binPath
}

type server struct {
	url    string
	exited chan error
	mu     sync.Mutex
	log    strings.Builder
}

func (s *server) logs() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.String()
}

// startServer runs the server binary against dbURL. If crashAt is set, the
// process exits abruptly when a leader reaches that phase.
func startServer(t *testing.T, dbURL string, crashAt idempotency.Phase) *server {
	t.Helper()
	cmd := exec.Command(serverBinary(t), "-addr", "127.0.0.1:0", "-store", "postgres", "-database-url", dbURL)
	cmd.Env = append(os.Environ(), crashEnv+"="+string(crashAt))
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	s := &server{exited: make(chan error, 1)}
	addr := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			line := sc.Text()
			s.mu.Lock()
			s.log.WriteString(line + "\n")
			s.mu.Unlock()
			if _, rest, ok := strings.Cut(line, "listening on "); ok {
				addr <- strings.Fields(rest)[0]
			}
		}
		s.exited <- cmd.Wait() // only after stderr is drained, as exec requires
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-s.exited
		if t.Failed() {
			t.Logf("server logs:\n%s", s.logs())
		}
	})

	select {
	case a := <-addr:
		s.url = "http://" + a
	case err := <-s.exited:
		t.Fatalf("server exited before listening: %v\n%s", err, s.logs())
	case <-time.After(15 * time.Second):
		t.Fatalf("server did not start\n%s", s.logs())
	}
	return s
}

// waitExit waits for a crashing server to die and checks it died at the failpoint.
func (s *server) waitExit(t *testing.T) {
	t.Helper()
	select {
	case err := <-s.exited:
		s.exited <- err // let Cleanup's receive succeed
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 3 {
			t.Fatalf("server exit = %v, want failpoint exit code 3\n%s", err, s.logs())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("server did not crash\n%s", s.logs())
	}
}

type result struct {
	status   int
	body     string
	replayed bool
}

func postCharge(t *testing.T, s *server, key string) (result, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, s.url+"/v1/charges", strings.NewReader(chargeBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(idempotency.HeaderName, key)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return result{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return result{resp.StatusCode, string(b), resp.Header.Get("Idempotent-Replayed") == "true"}, nil
}

func mustPost(t *testing.T, s *server, key string) result {
	t.Helper()
	r, err := postCharge(t, s, key)
	if err != nil {
		t.Fatalf("POST /v1/charges: %v\n%s", err, s.logs())
	}
	return r
}

func count(t *testing.T, sc *dbtest.Schema, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := sc.Pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// TestCrashBeforeCommitLeavesNoCharge: the server writes the charge inside its
// transaction and dies before committing. PostgreSQL must roll back both the
// charge and the key, and the client's retry must charge exactly once.
func TestCrashBeforeCommitLeavesNoCharge(t *testing.T) {
	t.Parallel()
	sc := dbtest.NewSchema(t)

	crashing := startServer(t, sc.URL, idempotency.PhaseExecuted)
	if _, err := postCharge(t, crashing, "crash-before-commit"); err == nil {
		t.Fatal("request to crashing server succeeded; failpoint did not fire")
	}
	crashing.waitExit(t)

	if n := count(t, sc, `SELECT count(*) FROM charges`); n != 0 {
		t.Fatalf("%d charges survived a crash before commit, want 0", n)
	}
	if n := count(t, sc, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("idempotency key survived a crash before commit")
	}

	restarted := startServer(t, sc.URL, "")
	retry := mustPost(t, restarted, "crash-before-commit")
	if retry.status != http.StatusCreated || retry.replayed {
		t.Fatalf("retry: status %d replayed=%t, want a fresh 201", retry.status, retry.replayed)
	}
	again := mustPost(t, restarted, "crash-before-commit")
	if again.status != http.StatusCreated || !again.replayed || again.body != retry.body {
		t.Fatalf("second retry: status %d replayed=%t body %q, want replay of %q", again.status, again.replayed, again.body, retry.body)
	}
	if n := count(t, sc, `SELECT count(*) FROM charges`); n != 1 {
		t.Fatalf("recorded %d charges, want exactly 1", n)
	}
}

// TestCrashAfterCommitReplaysOnRetry: the server commits the charge and dies
// before the client hears back. The client can't tell this from the case above,
// so it retries — and must get the original charge back, not a second one.
func TestCrashAfterCommitReplaysOnRetry(t *testing.T) {
	t.Parallel()
	sc := dbtest.NewSchema(t)

	crashing := startServer(t, sc.URL, idempotency.PhaseCommitted)
	if _, err := postCharge(t, crashing, "crash-after-commit"); err == nil {
		t.Fatal("request to crashing server succeeded; failpoint did not fire")
	}
	crashing.waitExit(t)

	if n := count(t, sc, `SELECT count(*) FROM charges`); n != 1 {
		t.Fatalf("%d charges after a crash after commit, want 1", n)
	}

	restarted := startServer(t, sc.URL, "")
	retry := mustPost(t, restarted, "crash-after-commit")
	if retry.status != http.StatusCreated || !retry.replayed {
		t.Fatalf("retry: status %d replayed=%t, want replayed 201", retry.status, retry.replayed)
	}
	if !strings.Contains(retry.body, `"amount_cents":4200`) {
		t.Fatalf("replayed body %q is not the original charge", retry.body)
	}
	if n := count(t, sc, `SELECT count(*) FROM charges`); n != 1 {
		t.Fatalf("retry after crash created another charge: %d rows, want 1", n)
	}
}
