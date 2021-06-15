package compressors

import (
	"compress/gzip"
	"io"
	"os"
)

func NewGzip(level int) Gzip {
	return Gzip{level}
}

type Gzip struct {
	Level int
}

func (Gzip) Name() string {
	return "Gzip Compressor"
}

func (f Gzip) Writer(path string, w io.Writer) (io.WriteCloser, error) {
	fw, err := gzip.NewWriterLevel(w, f.Level)
	if err != nil {
		return nil, err
	}
	return fw, nil
}

func (Gzip) Reader(path string, r io.Reader, _ os.FileInfo) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}
