package compressors

import (
	"io"

	"github.com/alpineiq/iodb/mw"
	"go.oneofone.dev/oerrs"
)

const (

	// ErrInvalidCompressor is returned when an invalid compressor is provided to NewCompressorByExt
	ErrInvalidCompressor = oerrs.String("invalid compressor")

	// ErrRawCompressor is returned when a raw compressor is provided to NewCompressorByExt
	ErrRawCompressor = oerrs.String("raw compressor provided")
)

// Reader / Writer / Closer
type rwc struct {
	w io.Writer
	r io.Reader
}

// Read will use the internal reader (if it's available) to perform the read functionality
func (r *rwc) Read(b []byte) (n int, err error) {
	if r.r == nil {
		err = io.EOF
		return
	}

	return r.r.Read(b)
}

// Write will use the internal writer (if it's available) to perform the write functionality
func (r *rwc) Write(b []byte) (n int, err error) {
	if r.w == nil {
		err = io.EOF
		return
	}

	return r.w.Write(b)
}

// Close will provide faux close functionality
func (r *rwc) Close() (err error) {
	return
}

// NewCompressorByExt returns a compressor middleware by extension
func NewCompressorByExt(ext string) (comp mw.Middleware, err error) {
	switch ext {
	case "gz", "gzip":
		comp = NewGzip(6)
	case "snappy":
		comp = NewSnappy()
	case "log", "txt", "raw":
		err = ErrRawCompressor
	default:
		err = ErrInvalidCompressor
	}

	return
}
