package idempotency

import "sync"

// flightResult is what a Postgres-backed leader hands to the duplicates that
// waited on it in the same process.
type flightResult struct {
	res      *response
	replayed bool // res was loaded from an earlier committed request
	conflict bool // the key belongs to a different request payload
	durable  bool // res reflects committed state and may be replayed to followers
}

// flight is one in-process execution of a key. Duplicates arriving while it runs
// wait on done instead of each taking a database connection to queue on the
// row lock.
type flight struct {
	hash string
	done chan struct{}
	out  flightResult // written before done is closed; read-only afterwards
}

// flightGroup coalesces concurrent duplicates within one process. Unlike Store
// it caches nothing: once a flight lands it is removed, and later requests go to
// PostgreSQL, which is the source of truth.
type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flight
}

// join returns the in-flight execution for key, creating one if there is none.
// leader reports whether the caller created it and must therefore call land.
func (g *flightGroup) join(key, hash string) (f *flight, leader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if f, ok := g.m[key]; ok {
		return f, false
	}
	if g.m == nil {
		g.m = make(map[string]*flight)
	}
	f = &flight{hash: hash, done: make(chan struct{})}
	g.m[key] = f
	return f, true
}

// land records the leader's outcome, removes the flight, and wakes its waiters.
func (g *flightGroup) land(key string, f *flight, out flightResult) {
	f.out = out
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	close(f.done)
}
