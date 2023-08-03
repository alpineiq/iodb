package iodb

import (
	"io"
	"os"
	"sync"
	"syscall"
)

type Reader struct {
	f      *file
	offset int64
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if n, err = syscall.Pread(r.f.Fd(), p, r.offset); err == nil && n == 0 {
		err = io.EOF
	}
	r.offset += int64(n)
	return
}

func (r *Reader) Close() error {
	r.f.close()
	return nil
}

func (r *Reader) Stat() os.FileInfo {
	return r.f.st
}

var _ io.ReadCloser = (*Reader)(nil)

func newROFile(path string, fs *files) *file {
	return &file{
		p:  path,
		fs: fs,
	}
}

type file struct {
	st      os.FileInfo
	f       *os.File
	fs      *files
	p       string
	mux     sync.Mutex
	readers int16
}

func (f *file) Fd() int {
	return int(f.f.Fd())
}

func (f *file) close() {
	f.mux.Lock()
	f.readers--
	if f.readers == 0 {
		f.fs.Delete(f.p)
		f.f.Close()
		f.f = nil
	}
	f.mux.Unlock()
}

func (f *file) Reader() (r *Reader, err error) {
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.f == nil {
		if f.f, err = os.Open(f.p); err != nil {
			if os.IsNotExist(err) {
				f.fs.Delete(f.p)
				err = os.ErrNotExist
			}
			return
		}
		if f.st, err = f.f.Stat(); err != nil {
			f.f.Close()
			f.f = nil
			return
		}
	}
	f.readers++
	r = &Reader{f: f}
	return
}

func newFiles() *files {
	return &files{m: map[string]*file{}}
}

type files struct {
	m   map[string]*file
	mux sync.RWMutex
}

func (fs *files) Get(path string) (rc *Reader, err error) {
	var ok bool
	fs.mux.RLock()
	f, ok := fs.m[path]
	fs.mux.RUnlock()
	if !ok {
		fs.mux.Lock()
		if f, ok = fs.m[path]; !ok { // in case it got loaded between the RUnlock and Lock
			f = newROFile(path, fs)
			fs.m[path] = f
		}
		fs.mux.Unlock()
	}
	return f.Reader()
}

func (fs *files) Delete(path string) {
	fs.mux.Lock()
	delete(fs.m, path)
	fs.mux.Unlock()
}
