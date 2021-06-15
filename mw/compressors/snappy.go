package compressors

import (
	"io"
	"os"

	"github.com/golang/snappy"
)

// NewSnappy returns a new instance of Snappy
func NewSnappy() Snappy {
	return Snappy{}
}

// Snappy is for the Google-created snappy compression type
type Snappy struct{}

// Name returns the name of the compressor type
func (Snappy) Name() string {
	return "Snappy Compressor"
}

// Writer returns a new Snappy writer
func (Snappy) Writer(path string, w io.Writer) (io.WriteCloser, error) {
	return snappy.NewWriter(w), nil
}

// Reader returns a new Snappy reader
func (Snappy) Reader(path string, r io.Reader, _ os.FileInfo) (io.ReadCloser, error) {
	return &rwc{r: snappy.NewReader(r)}, nil
}
