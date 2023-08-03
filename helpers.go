package iodb

import (
	"encoding/base64"
	"os"
	"sort"
	"strconv"
	"sync/atomic"

	"go.oneofone.dev/oerrs"
)

const (
	// ErrNoReaders is returned when a reader is requested, but none are available
	ErrNoReaders = oerrs.String("no readers available")

	// ErrClosing is returned when an action is performed while a database is shutting down
	ErrClosing = oerrs.String("database is shutting down")

	// ErrInvalidBucketType is returned when an invalid bucket type is provided
	ErrInvalidBucketType = oerrs.String("invalid bucket type")

	// ErrKeyExists is returned when the key exists for a write action with overwrite set to false
	ErrKeyExists = oerrs.String("key already exists")

	// ErrSamePath is returned when the same path is used for a bucket
	ErrSamePath = oerrs.String("same path")
)

func b64EncodeName(p string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(p))
}

func b64DecodeName(p string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type buckets map[string]*bucket

func (b buckets) Sort(rev bool) []string {
	out := make(sort.StringSlice, 0, len(b))
	for n := range b {
		out = append(out, n)
	}
	if rev {
		sort.Sort(sort.Reverse(out))
	} else {
		sort.Sort(out)
	}
	return []string(out)
}

type keyList map[string]os.FileInfo

func (k keyList) Names(rev bool) []string {
	out := make(sort.StringSlice, 0, len(k))
	for n := range k {
		out = append(out, n)
	}
	if rev {
		sort.Sort(sort.Reverse(out))
	} else {
		sort.Sort(out)
	}
	return []string(out)
}

func (k keyList) Paths(rev bool) []string {
	out := make(sort.StringSlice, 0, len(k))
	for _, p := range k {
		out = append(out, p.Name())
	}
	if rev {
		sort.Sort(sort.Reverse(out))
	} else {
		sort.Sort(out)
	}
	return []string(out)
}

var tmpFileCounter uint64

func tmpFileName(path string) string {
	return path + ".tmp." + strconv.FormatUint(atomic.AddUint64(&tmpFileCounter, 1), 16)
}

func lsDir(dir string) (files, dirs []os.FileInfo, err error) {
	var f *os.File
	if f, err = os.Open(dir); err != nil {
		return
	}
	defer f.Close()
	var st os.FileInfo
	if st, err = f.Stat(); err != nil || !st.IsDir() {
		return
	}
	var fis []os.FileInfo
	if fis, err = f.Readdir(-1); err != nil {
		return
	}

	for _, fi := range fis {
		if fn := fi.Name(); len(fn) == 0 || fn[0] == '.' {
			continue
		}
		switch {
		case fi.IsDir():
			dirs = append(dirs, fi)
		case fi.Mode().IsRegular():
			files = append(files, fi)
		}
	}

	return
}
