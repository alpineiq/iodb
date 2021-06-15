package iodb

import (
	"io"
	"os"
	"time"

	"github.com/alpineiq/iodb/mw"
)

type group struct {
	*bucket
	mw []mw.Middleware
}

func (g *group) Bucket(names ...string) Bucket {
	b := g.bucket.Bucket(names...)
	if b == nil {
		return nil
	}
	return &group{b.(*bucket), g.mw}
}

func (g *group) CreateBucket(names ...string) (Bucket, error) {
	b, err := g.bucket.CreateBucket(names...)
	if err != nil {
		return nil, err
	}
	return &group{b.(*bucket), g.mw}, nil
}

// Get returns an io.ReadCloser, it is the caller's responsibility to close the reader.
func (g *group) Get(key string, mws ...mw.Middleware) (rc io.ReadCloser, err error) {
	if len(mws) > 0 {
		return g.bucket.Get(key, mws...)
	}
	return g.bucket.Get(key, g.mw...)
}

func (g *group) GetAndDelete(key string, fn func(r io.Reader) error, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.GetAndDelete(key, fn, mws...)
	}
	return g.bucket.GetAndDelete(key, fn, g.mw...)
}

func (g *group) GetAndRename(key string, nBkt Bucket, nKey string, overwrite bool, fn ReaderFn, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.GetAndRename(key, nBkt, nKey, overwrite, fn, mws...)
	}
	return g.bucket.GetAndRename(key, nBkt, nKey, overwrite, fn, g.mw...)
}

func (g *group) PutTimedFunc(key string, fn func(io.Writer) error, expiry time.Duration, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.PutTimedFunc(key, fn, expiry, mws...)
	}
	return g.bucket.PutTimedFunc(key, fn, expiry, g.mw...)
}

func (g *group) PutFunc(key string, fn func(io.Writer) error, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.PutFunc(key, fn, mws...)
	}
	return g.bucket.PutFunc(key, fn, g.mw...)
}

func (g *group) Put(key string, r io.Reader, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.Put(key, r, mws...)
	}
	return g.bucket.Put(key, r, g.mw...)
}

func (g *group) PutTimed(key string, r io.Reader, expiry time.Duration, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.PutTimed(key, r, expiry, mws...)
	}
	return g.bucket.PutTimed(key, r, expiry, g.mw...)
}

func (g *group) AppendFunc(key string, fn func(io.Writer) error, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.AppendFunc(key, fn, mws...)
	}
	return g.bucket.AppendFunc(key, fn, g.mw...)
}

func (g *group) Append(key string, r io.Reader, mws ...mw.Middleware) (err error) {
	if len(mws) > 0 {
		return g.bucket.Append(key, r, mws...)
	}
	return g.bucket.Append(key, r, g.mw...)
}

func (g *group) ForEach(fn func(key string, value io.Reader) error, mws ...mw.Middleware) error {
	if len(mws) > 0 {
		return g.bucket.ForEach(fn, mws...)
	}
	return g.bucket.ForEach(fn, g.mw...)
}

func (g *group) ForEachReverse(fn func(key string, value io.Reader) error, mws ...mw.Middleware) error {
	if len(mws) > 0 {
		return g.bucket.ForEachReverse(fn, mws...)
	}
	return g.bucket.ForEachReverse(fn, g.mw...)
}

func (g *group) Group(mws ...mw.Middleware) Bucket {
	return &group{g.bucket, mws}
}

func (g *group) Stat(key string) (fi os.FileInfo, err error) {
	var ok bool
	g.mux.RLock()
	if fi, ok = g.keys[key]; !ok {
		err = ErrFileDoesNotExist
	}

	g.mux.RUnlock()
	return
}
