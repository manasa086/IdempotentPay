package idempotency

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoreBeginLeaderThenFollower(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	t.Cleanup(s.Close)

	e1, oc1 := s.begin("k", "hash-a")
	if oc1 != outcomeLeader {
		t.Fatalf("first begin: outcome %v, want leader", oc1)
	}

	e2, oc2 := s.begin("k", "hash-a")
	if oc2 != outcomeFollower {
		t.Fatalf("second begin: outcome %v, want follower", oc2)
	}
	if e1 != e2 {
		t.Fatalf("follower got a different entry than the leader")
	}

	res := &response{status: http.StatusCreated, header: http.Header{}, body: []byte("done")}
	s.complete("k", res)

	select {
	case <-e2.done:
	case <-time.After(time.Second):
		t.Fatal("follower was not woken by complete")
	}
	if e2.result != res {
		t.Fatalf("follower saw result %+v, want %+v", e2.result, res)
	}
}

func TestStoreConflictOnDifferentHash(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	t.Cleanup(s.Close)

	if _, oc := s.begin("k", "hash-a"); oc != outcomeLeader {
		t.Fatalf("first begin outcome %v, want leader", oc)
	}
	if _, oc := s.begin("k", "hash-b"); oc != outcomeConflict {
		t.Fatalf("mismatched-hash begin outcome %v, want conflict", oc)
	}
}

func TestStoreAbortReleasesKeyAndWakesFollowers(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	t.Cleanup(s.Close)

	leader, _ := s.begin("k", "h")
	follower, oc := s.begin("k", "h")
	if oc != outcomeFollower {
		t.Fatalf("outcome %v, want follower", oc)
	}

	s.abort("k")

	select {
	case <-follower.done:
	case <-time.After(time.Second):
		t.Fatal("abort did not wake follower")
	}
	if follower.result != nil {
		t.Fatalf("aborted entry has result %+v, want nil", follower.result)
	}
	if leader.result != nil {
		t.Fatalf("leader entry has result %+v, want nil", leader.result)
	}

	// Key is free again: the next caller becomes a leader.
	if _, oc := s.begin("k", "h"); oc != outcomeLeader {
		t.Fatalf("post-abort begin outcome %v, want leader", oc)
	}
}

func TestStoreExpiryPromotesNextCallerToLeader(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := NewStore(time.Hour)
	t.Cleanup(s.Close)
	s.now = func() time.Time { return now }

	s.begin("k", "h")
	s.complete("k", &response{status: 200, header: http.Header{}, body: []byte("v1")})

	// Still fresh: a duplicate is a follower that replays.
	if _, oc := s.begin("k", "h"); oc != outcomeFollower {
		t.Fatalf("within ttl: outcome %v, want follower", oc)
	}

	now = now.Add(2 * time.Hour) // advance past the TTL

	if _, oc := s.begin("k", "h"); oc != outcomeLeader {
		t.Fatalf("after ttl: outcome %v, want leader", oc)
	}
}

func TestStoreSweepEvictsExpiredEntries(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := NewStore(time.Hour)
	t.Cleanup(s.Close)
	s.now = func() time.Time { return now }

	s.begin("done", "h")
	s.complete("done", &response{status: 200, header: http.Header{}, body: nil})
	s.begin("inflight", "h") // never completed

	now = now.Add(2 * time.Hour)
	s.sweep()

	if _, ok := s.m["done"]; ok {
		t.Errorf("expired completed entry survived sweep")
	}
	if _, ok := s.m["inflight"]; !ok {
		t.Errorf("in-flight entry must never be swept")
	}
}

func TestStoreExpiryDisabledWhenTTLZero(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	t.Cleanup(s.Close)

	e, _ := s.begin("k", "h")
	s.complete("k", &response{status: 200, header: http.Header{}, body: nil})
	if s.isExpiredLocked(e) {
		t.Fatal("entry reported expired with TTL disabled")
	}
}

// TestStoreBeginIsSingleLeaderUnderRace hammers begin from many goroutines and
// asserts that exactly one of them is elected leader for the key.
func TestStoreBeginIsSingleLeaderUnderRace(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	t.Cleanup(s.Close)

	const n = 500
	var leaders atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, oc := s.begin("k", "h"); oc == outcomeLeader {
				leaders.Add(1)
			}
		}()
	}
	wg.Wait()

	if leaders.Load() != 1 {
		t.Fatalf("elected %d leaders, want exactly 1", leaders.Load())
	}
}

func TestStoreCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	s := NewStore(time.Hour)
	s.Close()
	s.Close() // must not panic on a double close
}
