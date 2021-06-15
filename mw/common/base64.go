package common

import (
	"encoding/base64"
	"io/ioutil"
	"os"

	"io"
)

func NewBase64() Base64 {
	return NewBase64Encoding(base64.URLEncoding)
}

func NewBase64Encoding(enc *base64.Encoding) Base64 {
	return Base64{enc}
}

type Base64 struct {
	enc *base64.Encoding
}

func (Base64) Name() string {
	return "Base64 Middleware"
}

func (b Base64) Writer(path string, w io.Writer) (io.WriteCloser, error) {
	return base64.NewEncoder(b.enc, w), nil
}

func (b Base64) Reader(path string, r io.Reader, _ os.FileInfo) (io.ReadCloser, error) {
	return ioutil.NopCloser(base64.NewDecoder(b.enc, r)), nil
}
