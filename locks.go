package iodb

import (
	"sync"
	"sync/atomic"
	"time"
)

const cleanupDuration = 10 * time.Minute

func newPathLocker() (pl *pathLocker) {
	pl = &pathLocker{
		m: map[string]*rwlock{},
		t: time.NewTicker(cleanupDuration),
	}
	go func() {
		for range pl.t.C {
			pl.mux.Lock()
			pl.purge()
			pl.mux.Unlock()
		}
	}()
	return
}

type pathLocker struct {
	m   map[string]*rwlock
	t   *time.Ticker
	mux sync.Mutex
}

// get is *only* called internally by Lock(p) / RLock(p)
func (pl *pathLocker) get(p string) (l *rwlock, ok bool) {
	pl.mux.Lock()
	defer pl.mux.Unlock()
	if l, ok = pl.m[p]; !ok {
		l = &rwlock{}
		pl.m[p] = l
	}
	l.inc()
	return
}

func (pl *pathLocker) Lock(p string) (l *rwlock) {
	l, _ = pl.get(p)
	l.mux.Lock()
	return
}

func (pl *pathLocker) RLock(p string) (l *rwlock) {
	l, _ = pl.get(p)
	l.mux.RLock()
	return
}

// Close blocks until all locks are cleared.
func (pl *pathLocker) Close() error { // provide Closer interface
	pl.mux.Lock()
	defer pl.mux.Unlock()

	pl.t.Stop()

	for len(pl.m) > 0 {
		pl.purge()
		time.Sleep(time.Millisecond) // don't burn the cpu
	}

	return nil
}

func (pl *pathLocker) purge() {
	for k, l := range pl.m {
		if !l.IsActive() {
			delete(pl.m, k)
		}
	}
}

type rwlock struct {
	mux    sync.RWMutex
	active int64
}

func (rw *rwlock) inc() {
	atomic.AddInt64(&rw.active, 1)
}

func (rw *rwlock) Unlock() {
	rw.mux.Unlock()
	atomic.AddInt64(&rw.active, -1)
}

func (rw *rwlock) RUnlock() {
	rw.mux.RUnlock()
	atomic.AddInt64(&rw.active, -1)
}

func (rw *rwlock) IsActive() bool {
	return atomic.LoadInt64(&rw.active) != 0
}
