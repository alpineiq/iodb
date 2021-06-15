package mw

import (
	"io"
	"os"
)

// Middleware is the interface that defines an encoder/decoder chain.
type Middleware interface {
	Name() string
	Writer(path string, w io.Writer) (io.WriteCloser, error) // calling Close on the returned writer must ***NOT*** close the parent.
	Reader(path string, r io.Reader, st os.FileInfo) (io.ReadCloser, error)
}
