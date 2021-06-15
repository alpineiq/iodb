package compressors

import (
	"compress/flate"
	"io/ioutil"
	"os"

	"io"
)

func NewFlate(level int) Flate {
	return Flate{level}
}

type Flate struct {
	Level int
}

func (Flate) Name() string {
	return "Flate Compressor"
}

func (f Flate) Writer(path string, w io.Writer) (io.WriteCloser, error) {
	fw, err := flate.NewWriter(w, f.Level)
	if err != nil {
		return nil, err
	}
	return fw, nil
}

func (Flate) Reader(path string, r io.Reader, _ os.FileInfo) (io.ReadCloser, error) {
	return ioutil.NopCloser(flate.NewReader(r)), nil
}
