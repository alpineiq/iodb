package iodb

import (
	"io"

	"github.com/alpineiq/iodb/mw"
	"go.oneofone.dev/oerrs"
)

// MiddlewareError represents an error that happened in a middleware
type MiddlewareError struct {
	Mw  mw.Middleware
	Err error
}

func (wr *MiddlewareError) Error() string {
	return wr.Mw.Name() + ":  " + wr.Err.Error()
}

type writerChain []io.WriteCloser

func (wc writerChain) Write(p []byte) (int, error) {
	return wc[len(wc)-1].Write(p)
}

func (wc writerChain) Close() error {
	var errl oerrs.ErrorList
	for i := len(wc) - 1; i >= 0; i-- {
		errl.PushIf(wc[i].Close())
	}
	return errl.Err()
}

type readerChain []io.ReadCloser

func (rc readerChain) Read(p []byte) (int, error) {
	return rc[len(rc)-1].Read(p)
}

func (rc readerChain) Close() error {
	var errl oerrs.ErrorList
	for i := len(rc) - 1; i >= 0; i-- {
		errl.PushIf(rc[i].Close())
	}
	return errl.Err()
}

type middlewareList []mw.Middleware

func (mwl middlewareList) applyWriters(path string, w io.WriteCloser) (io.WriteCloser, error) {
	wc := make(writerChain, 0, len(mwl)+1)
	wc = append(wc, w)
	for _, mw := range mwl {
		mww, err := mw.Writer(path, w)
		if err != nil {
			wc.Close()
			return nil, &MiddlewareError{mw, err}
		}
		w = mww
		wc = append(wc, w)
	}
	return wc, nil
}

func (mwl middlewareList) applyReaders(path string, rd *Reader) (io.ReadCloser, error) {
	var (
		r  io.ReadCloser = rd
		rc               = append(make(readerChain, 0, len(mwl)+1), rd)
		st               = rd.Stat()
	)

	for _, mw := range mwl {
		mwr, err := mw.Reader(path, r, st)
		if err != nil {
			rc.Close()
			return nil, &MiddlewareError{mw, err}
		}
		r = mwr
		rc = append(rc, r)
	}

	return rc, nil
}
