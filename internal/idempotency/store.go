// Package idempotency provides a concurrency-safe idempotency layer for REST
// APIs that perform side effects (charges, transfers, order creation).
//
// It is built around two guarantees:
//
//  1. Deduplication: once a request carrying a given Idempotency-Key has been
//     processed, later requests with the same key replay the stored response
//     instead of re-running the handler.
//
//  2. Request coalescing: while the first ("leader") request for a key is still
//     in flight, concurrent duplicates block on that single execution rather
//     than racing into the handler independently. The handler therefore runs
//     exactly once per key, even under a burst of simultaneous retries.
//
// The store keeps state in memory, guarded by a single mutex. It is intended
// for a single process; a multi-instance deployment would back it with a shared
// store (Redis, Postgres) exposing the same begin/complete/abort protocol.
package idempotency

import (
	"net/http"
	"sync"
	"time"
)

// response is the cached outcome of a fully processed request.
type response struct {
	status int
	header http.Header
	body   []byte
}

// entry tracks one Idempotency-Key through its lifetime: first while the leader
// request runs, then as a cached response that duplicates replay.
//
// done is closed exactly once, by whichever call (complete or abort) resolves
// the leader's execution. Closing done establishes a happens-before edge, so
// any goroutine that observes done closed may read result without further
// synchronization. A non-nil result means "replay this"; a nil result means the
// leader failed and the key was released for a fresh attempt.
type entry struct {
	requestHash string
	done        chan struct{}
	result      *response
	createdAt   time.Time
}

// Store is an in-memory, mutex-guarded registry of Idempotency-Key entries.
// The zero value is not usable; construct one with NewStore.
type Store struct {
	mu  sync.Mutex
	m   map[string]*entry
	ttl time.Duration

	now func() time.Time // overridable in tests

	stop     chan struct{}
	stopOnce sync.Once
	janitor  sync.WaitGroup
}

// NewStore returns a ready Store. If ttl > 0, completed entries are eligible for
// eviction ttl after they were created, and a background janitor sweeps expired
// entries roughly every ttl. If ttl <= 0, entries are retained for the lifetime
// of the process.
func NewStore(ttl time.Duration) *Store {
	s := &Store{
		m:    make(map[string]*entry),
		ttl:  ttl,
		now:  time.Now,
		stop: make(chan struct{}),
	}
	if ttl > 0 {
		s.janitor.Add(1)
		go s.sweepLoop(ttl)
	}
	return s
}

// Close stops the background janitor. It is safe to call more than once. In-flight
// requests are unaffected; the store simply stops evicting expired entries.
func (s *Store) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.janitor.Wait()
}

// outcome describes what begin decided for a caller.
type outcome int

const (
	// outcomeLeader: no live entry existed. The caller owns the execution and
	// must call complete or abort for this key exactly once.
	outcomeLeader outcome = iota
	// outcomeFollower: a live entry exists for the same request. The caller must
	// not run the handler; it should wait on entry.done and replay entry.result.
	outcomeFollower
	// outcomeConflict: a live entry exists for this key but was created for a
	// different request payload. The caller should reject the request.
	outcomeConflict
)

// begin registers intent to process reqHash under key.
//
// The returned entry is non-nil for outcomeLeader and outcomeFollower. For a
// leader it is freshly created and already stored; for a follower it is the
// existing entry to wait on.
func (s *Store) begin(key, reqHash string) (*entry, outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.m[key]; ok {
		if !s.isExpiredLocked(e) {
			if e.requestHash != reqHash {
				return nil, outcomeConflict
			}
			return e, outcomeFollower
		}
		// Expired completed entry: fall through and replace it so correctness
		// never depends on the janitor having run.
		delete(s.m, key)
	}

	e := &entry{
		requestHash: reqHash,
		done:        make(chan struct{}),
		createdAt:   s.now(),
	}
	s.m[key] = e
	return e, outcomeLeader
}

// complete records res as the cached response for key and wakes any followers.
// It must be called at most once per successful leader execution.
func (s *Store) complete(key string, res *response) {
	s.mu.Lock()
	e := s.m[key]
	s.mu.Unlock()
	if e == nil {
		return // evicted; nothing waiting that we can help
	}
	e.result = res
	close(e.done)
}

// abort releases key without caching a response and wakes any followers, which
// will then see a nil result and surface a retryable error. It is used when the
// leader panics, the client disconnects, or the handler returns a server error
// that should not be memoized.
func (s *Store) abort(key string) {
	s.mu.Lock()
	e := s.m[key]
	if e != nil {
		delete(s.m, key)
	}
	s.mu.Unlock()
	if e != nil {
		close(e.done)
	}
}

// Len reports the number of tracked entries (in-flight plus cached). Primarily
// useful for tests and metrics.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

func (s *Store) isExpiredLocked(e *entry) bool {
	if s.ttl <= 0 || e.result == nil {
		return false // no TTL, or still in flight
	}
	return s.now().Sub(e.createdAt) >= s.ttl
}

func (s *Store) sweepLoop(interval time.Duration) {
	defer s.janitor.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.m {
		if s.isExpiredLocked(e) {
			delete(s.m, k)
		}
	}
}
