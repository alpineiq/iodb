package iodb

import (
	"archive/tar"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alpineiq/iodb/mw"
	"go.oneofone.dev/oerrs"
)

type bucket struct {
	name string
	path string

	files *files

	db *DB

	keys    keyList
	buckets buckets

	mux sync.RWMutex

	meta *metadata
}

func newBucket(name, parentPath string, db *DB) (b *bucket, err error) {
	path := filepath.Join(parentPath, db.encodeKey(name))
	if err = os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	b = &bucket{
		name: name,
		path: path,
		db:   db,

		files: newFiles(db.maxFilesSem),
	}

	if b.meta, err = loadMetadata(b.path); err != nil {
		return nil, err
	}

	if err = b.reload(); err != nil {
		b = nil
	}

	return
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) Path() string {
	return b.path
}

func (b *bucket) Bucket(names ...string) Bucket {
	if len(names) == 0 {
		return b
	}
	b.mux.RLock()
	defer b.mux.RUnlock()
	var cb *bucket
	if cb = b.buckets[names[0]]; cb != nil {
		if len(names) > 1 {
			return cb.Bucket(names[1:]...)
		}
	} else {
		return nil
	}
	return cb
}

func (b *bucket) CreateBucket(names ...string) (_ Bucket, err error) {
	if len(names) == 0 {
		return b, nil
	}
	var ok bool
	name := names[0]
	b.mux.Lock()
	defer b.mux.Unlock()
	var cb *bucket
	if cb, ok = b.buckets[name]; !ok {
		if cb, err = newBucket(name, b.path, b.db); err == nil {
			b.buckets[name] = cb
		} else {
			return
		}
	}
	if cb != nil && len(names) > 1 {
		return cb.CreateBucket(names[1:]...)
	}

	return cb, err
}

func (b *bucket) DeleteBucket(name string) (err error) {
	b.mux.Lock()
	if cb, ok := b.buckets[name]; ok {
		delete(b.buckets, name)
		err = os.RemoveAll(cb.path)
	} else {
		err = os.ErrNotExist
	}
	b.mux.Unlock()
	return
}

// Get returns an io.ReadCloser, it is the caller's responsibility to close the reader.
func (b *bucket) Get(key string, middlewares ...mw.Middleware) (_ io.ReadCloser, err error) {
	b.mux.RLock()
	fi, ok := b.keys[key]
	b.mux.RUnlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	var (
		rd   *Reader
		fn   = fi.Name()
		path = filepath.Join(b.path, fn)
	)

	defer b.db.lk.RLock(path).RUnlock()
	if rd, err = b.files.Get(path); err != nil {
		return
	}

	return middlewareList(middlewares).applyReaders(fn, rd)
}

func (b *bucket) PutTimedFunc(key string, fn func(w io.Writer) error, expireAfter time.Duration, middlewares ...mw.Middleware) (err error) {
	if !b.db.maxFilesSem.Acquire() {
		return ErrClosing
	}

	defer b.db.maxFilesSem.Release()
	var (
		encKey  = b.db.encodeKey(key)
		path    = filepath.Join(b.path, encKey)
		tmpPath = tmpFileName(path)
		f       *os.File
	)
	defer b.db.lk.Lock(path).Unlock()

	if f, err = os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644); err != nil {
		return
	}
	defer os.Remove(tmpPath) // this will error if os.Rename doesn't fail, which is fine

	var wc io.WriteCloser
	if wc, err = middlewareList(middlewares).applyWriters(path, f); err != nil {
		return
	}

	if err = fn(wc); err != nil {
		wc.Close()
		return
	}

	if err = wc.Close(); err != nil {
		return
	}

	b.mux.Lock()
	defer b.mux.Unlock()
	if err = os.Rename(tmpPath, path); err != nil {
		return
	}
	var st os.FileInfo
	if st, err = os.Stat(path); err != nil {
		return
	}
	if _, ok := b.keys[key]; !ok { // only increase the counter if new files
		b.meta.incCounter()
	}
	b.keys[key] = st
	if expireAfter > 0 {
		b.meta.SetExpiryDate(key, time.Now().Add(expireAfter).Unix())
		ts := st.ModTime()
		time.AfterFunc(expireAfter, func() { b.deleteTimed(key, ts) })
	} else {
		b.meta.SetExpiryDate(key, 0) // this is needed in case you changed the expiry.
	}
	err = b.meta.store()

	return
}

func (b *bucket) PutFunc(key string, fn func(w io.Writer) error, middlewares ...mw.Middleware) (err error) {
	return b.PutTimedFunc(key, fn, 0, middlewares...)
}

func (b *bucket) PutTimed(key string, r io.Reader, expireAfter time.Duration, middlewares ...mw.Middleware) (err error) {
	fn := func(w io.Writer) error { _, err := io.Copy(w, r); return err }
	return b.PutTimedFunc(key, fn, expireAfter, middlewares...)
}

func (b *bucket) Put(key string, r io.Reader, middlewares ...mw.Middleware) (err error) {
	fn := func(w io.Writer) error { _, err := io.Copy(w, r); return err }
	return b.PutTimedFunc(key, fn, 0, middlewares...)
}

func (b *bucket) Append(key string, r io.Reader, middlewares ...mw.Middleware) (err error) {
	fn := func(w io.Writer) error { _, err := io.Copy(w, r); return err }
	return b.AppendFunc(key, fn, middlewares...)
}

func (b *bucket) AppendFunc(key string, fn func(w io.Writer) error, middlewares ...mw.Middleware) (err error) {
	if !b.db.maxFilesSem.Acquire() {
		return ErrClosing
	}

	defer b.db.maxFilesSem.Release()
	var (
		encKey = b.db.encodeKey(key)
		path   = filepath.Join(b.path, encKey)
		f      *os.File
		wc     io.WriteCloser
	)
	defer b.db.lk.Lock(path).Unlock()
	if f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644); err != nil {
		return
	}

	if wc, err = middlewareList(middlewares).applyWriters(path, f); err != nil {
		return
	}

	if err = fn(wc); err != nil {
		wc.Close()
		return
	}

	if err = wc.Close(); err != nil {
		return
	}

	b.mux.Lock()
	defer b.mux.Unlock()
	if _, ok := b.keys[key]; !ok { // only increase the counter if new files
		b.meta.incCounter()
	}

	var st os.FileInfo

	if st, err = os.Stat(path); err != nil {
		return
	}

	b.keys[key] = st
	b.meta.SetExpiryDate(key, 0)

	return b.meta.store()
}

func (b *bucket) GetAndDelete(key string, fn func(r io.Reader) error, middlewares ...mw.Middleware) (err error) {
	b.mux.RLock()
	fi, ok := b.keys[key]
	b.mux.RUnlock()

	if !ok {
		return os.ErrNotExist
	}
	var (
		rd    *Reader
		fname = fi.Name()
		path  = filepath.Join(b.path, fname)
		rc    io.ReadCloser
	)

	defer b.db.lk.Lock(path).Unlock()

	if rd, err = b.files.Get(path); err != nil {
		return
	}

	if rc, err = middlewareList(middlewares).applyReaders(fname, rd); err != nil {
		return
	}

	if err = fn(rc); err != nil {
		rc.Close()
		return
	}
	rc.Close()

	b.mux.Lock()
	err = os.Remove(path)
	b.nukeKey(key)
	b.files.Delete(path)
	b.mux.Unlock()

	return
}

// ReaderFn is used for multi-use lock actions (Such as GetAndDelete and GetAndRename)
type ReaderFn func(io.Reader) error

func (b *bucket) GetAndRename(key string, nBkt Bucket, nKey string, overwrite bool, fn ReaderFn, mws ...mw.Middleware) (err error) {
	b.mux.RLock()
	fi, ok := b.keys[key]
	b.mux.RUnlock()

	if !ok {
		return os.ErrNotExist
	}

	if !b.db.maxFilesSem.Acquire() {
		return ErrClosing
	}

	defer b.db.maxFilesSem.Release()

	var (
		rd    *Reader
		fname = fi.Name()
		path  = filepath.Join(b.path, fname)
		rc    io.ReadCloser
		nb    *bucket
	)

	switch v := nBkt.(type) {
	case *bucket:
		nb = v
	case *group:
		nb = v.bucket
	default:
		return ErrInvalidBucketType
	}

	var (
		nf    *file
		nPath = filepath.Join(nb.path, nKey)
	)

	defer nb.db.lk.Lock(nPath).Unlock()

	defer b.db.lk.Lock(path).Unlock()

	if rd, err = b.files.Get(path); err != nil {
		return
	}

	if rc, err = middlewareList(mws).applyReaders(fname, rd); err != nil {
		return
	}

	nb.mux.Lock()
	nb.files.mux.Lock()

	if nf, ok = nb.files.m[nKey]; !ok {
		// Our new file doesn't exist, so let's do the appropriate actions to add it
		nf = newROFile(nPath, nb.files)
		nb.files.m[nKey] = nf
	} else if !overwrite {
		nb.files.mux.Unlock()
		nb.mux.Unlock()
		rc.Close()
		return ErrKeyExists
	}

	// Now that we have the new file, let's lock it before unlocking it's parents
	nf.mux.Lock()
	// We will ensure we always unlock once this function exits
	defer nf.mux.Unlock()

	// Release new bucket files mutex
	nb.files.mux.Unlock()
	// Release the new bucket mutex
	nb.mux.Unlock()

	if err = fn(rc); err != nil {
		rc.Close()
		return
	}
	rc.Close()

	b.mux.Lock()
	defer b.mux.Unlock()

	if b != nb { // make sure it's not the same bucket or we will get a deadlock
		nb.mux.Lock()
		defer nb.mux.Unlock()
	}

	if err = os.Rename(path, nPath); err != nil {
		return
	}

	var st os.FileInfo
	if st, err = os.Stat(nPath); err != nil {
		return
	}

	delete(b.keys, key)
	b.files.Delete(path)
	nb.keys[nKey] = st
	if !ok {
		nb.meta.incCounter()
	}
	return
}

func (b *bucket) Delete(key string) (err error) {
	b.mux.Lock()
	if fi, ok := b.keys[key]; ok {
		path := filepath.Join(b.path, fi.Name())
		defer b.db.lk.Lock(path).Unlock()
		err = os.Remove(path)
		delete(b.keys, key)
		b.files.Delete(path)
	}
	b.mux.Unlock()
	return
}

func (b *bucket) Rename(key, nkey string) (err error) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if fi, ok := b.keys[key]; ok {
		path := filepath.Join(b.path, fi.Name())
		defer b.db.lk.Lock(path).Unlock()
		npath := filepath.Join(b.path, filepath.Clean(nkey))
		defer b.db.lk.Lock(npath).Unlock()
		if err = os.Rename(path, npath); err != nil {
			return
		}

		delete(b.keys, key)
		b.files.Delete(path)

		var st os.FileInfo
		if st, err = os.Stat(npath); err != nil {
			return
		}
		b.keys[nkey] = st
	}
	return
}

func (b *bucket) deleteTimed(key string, ct time.Time) {
	now := time.Now().Unix()

	b.mux.Lock()
	defer b.mux.Unlock()
	if fi, ok := b.keys[key]; ok {
		var exp int64
		if exp, ok = b.meta.ExpiryDate[key]; !ok {
			return
		}

		if exp == 0 || now < exp {
			return
		}

		if !fi.ModTime().Equal(ct) { // if the file was modified, don't delete it
			return
		}

		path := filepath.Join(b.path, fi.Name())
		os.Remove(path)
		b.nukeKey(key)
		b.files.Delete(path)
	}
}

func (b *bucket) nukeKey(key string) {
	delete(b.keys, key)
	delete(b.meta.ExpiryDate, key)
	delete(b.meta.Extra, key)
}

func (b *bucket) Buckets(rev bool) (out []string) {
	b.mux.RLock()
	out = b.buckets.Sort(rev)
	b.mux.RUnlock()
	return
}

func (b *bucket) Keys(reverse bool) (out []string) {
	b.mux.RLock()
	out = b.keys.Names(reverse)
	b.mux.RUnlock()
	return
}

func (b *bucket) AllExtraData() (out map[string]map[string]string) {
	out = make(map[string]map[string]string)
	b.mux.RLock()
	for _, p := range b.keys.Names(false) {
		out[p] = b.meta.CopyExtra(p)
	}
	b.mux.RUnlock()
	return
}

func (b *bucket) reload() error {
	files, dirs, err := lsDir(b.path)
	if err != nil {
		log.Println(err)
		return err
	}

	b.keys = make(keyList, len(files)) // it could be overallocated if we have child buckets / metadata but that's ok
	b.buckets = make(buckets, len(dirs))
	now := time.Now().Unix()
	for _, fi := range files {
		fn := fi.Name()
		key, err := b.db.decodeKey(fn)
		if err != nil {
			continue
		}
		if ts, ok := b.meta.ExpiryDate[key]; ok {
			if ts != 0 && ts <= now {
				os.Remove(filepath.Join(b.path, fn))
			}
		}
		b.keys[key] = fi
	}

	for _, fi := range dirs {
		key, err := b.db.decodeKey(fi.Name())
		if err != nil {
			continue
		}
		if _, err = b.CreateBucket(key); err != nil {
			log.Printf("wtfmate %v", err)
		}
	}

	return nil
}

// ForEach loops over all the keys in the bucket in order and calls fn with a reader.
// *note* this function read-locks the bucket.
func (b *bucket) ForEach(fn func(key string, value io.Reader) error, middlewares ...mw.Middleware) error {
	return b.forEach(false, fn, middlewares...)
}

// ForEachReverse loops over all the keys in the bucket in order and calls fn with a reader.
// *note* this function read-locks the bucket.
func (b *bucket) ForEachReverse(fn func(key string, value io.Reader) error, middlewares ...mw.Middleware) error {
	return b.forEach(true, fn, middlewares...)
}

func (b *bucket) forEach(rev bool, fn func(key string, value io.Reader) error, middlewares ...mw.Middleware) error {
	b.mux.RLock()
	defer b.mux.RUnlock()

	for _, k := range b.keys.Names(rev) {
		path := filepath.Join(b.path, b.keys[k].Name())
		rd, err := b.files.Get(path)
		if err != nil { // should we return this error? it means the file could have been moved / deleted
			continue
		}
		var rc io.ReadCloser
		if rc, err = middlewareList(middlewares).applyReaders(path, rd); err != nil {
			return err
		}
		func() {
			defer recover()
			err = fn(k, rc)
		}()
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *bucket) NextID() *big.Int {
	n := big.NewInt(0)
	n.SetString(b.meta.Counter.String(), 10)
	return n
}

func (b *bucket) Group(mws ...mw.Middleware) Bucket {
	return &group{b, mws}
}

func (b *bucket) Export(w io.Writer) (err error) {
	var (
		tw          *tar.Writer
		el          oerrs.ErrorList
		rootPathLen = len(b.db.root.path) + 1 // strip the physical part of the path
	)

	if otw, ok := w.(*tar.Writer); ok {
		tw = otw
	} else {
		tw = tar.NewWriter(w)
		defer func() { el.PushIf(tw.Close()); err = el.Err() }()
	}

	b.mux.Lock()
	defer b.mux.Unlock()

	for _, n := range b.keys.Names(false) {
		var (
			fi      = b.keys[n]
			path    = filepath.Join(b.path, fi.Name())
			tarPath = path[rootPathLen:]
			hdr     *tar.Header
			rd      *Reader
		)
		if hdr, err = tar.FileInfoHeader(fi, ""); err != nil {
			el.PushIf(err)
			return
		}
		hdr.Name = tarPath

		if err = tw.WriteHeader(hdr); err != nil {
			el.PushIf(err)
			return
		}
		if rd, err = b.files.Get(path); err != nil {
			log.Printf("err [%s, %s]: %v", n, tarPath, err)
			continue
		}
		_, err = io.Copy(tw, rd)
		rd.Close()
		if err != nil {
			el.PushIf(err)
			return
		}
	}

	for _, cb := range b.buckets {
		if err = cb.Export(tw); err != nil {
			el.PushIf(err)
			return
		}
	}

	return
}

func (b *bucket) Stat(key string) (fi os.FileInfo, err error) {
	var ok bool
	b.mux.RLock()
	if fi, ok = b.keys[key]; !ok {
		err = ErrFileDoesNotExist
	}
	b.mux.RUnlock()
	return
}

// SetExtraData sets extra meta data on the specified file.
// pass nil to val to delete the data associated with the key.
func (b *bucket) SetExtraData(fileKey, key string, val string) error {
	b.mux.Lock()
	defer b.mux.Unlock()

	if _, ok := b.keys[fileKey]; !ok {
		return os.ErrNotExist
	}

	b.meta.SetExtraData(fileKey, key, val)
	return b.meta.store()
}

func (b *bucket) GetExtraData(fileKey, key string) (out string) {
	b.mux.RLock()
	out = b.meta.Extra[fileKey][key]
	b.mux.RUnlock()

	return
}

func (b *bucket) ExtraData(fileKey string) (out map[string]string) {
	b.mux.RLock()
	defer b.mux.RUnlock()

	d := b.meta.Extra[fileKey]
	out = make(map[string]string, len(d))

	for k, v := range out {
		out[k] = v
	}

	return
}
