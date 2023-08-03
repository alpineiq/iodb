package iodb

import (
	"io"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/alpineiq/iodb/mw"
	"go.oneofone.dev/oerrs"
)

// ErrFileDoesNotExist is returned when a file does not exist
const ErrFileDoesNotExist = oerrs.String("file does not exist")

// Options allows a bit of customization for iodb.
type Options struct {
	Middleware     []mw.Middleware
	PlainFileNames bool
}

var defOpts = Options{}

type DB struct {
	root *bucket
	opts *Options
	lk   *pathLocker
}

func New(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &defOpts
	}

	db := &DB{
		opts: opts,
		lk:   newPathLocker(),
	}
	b, err := newBucket("", path, db)
	if err != nil {
		return nil, err
	}
	db.root = b
	return db, nil
}

func (db *DB) Bucket(names ...string) Bucket {
	return db.root.Bucket(names...)
}

// Import imports the database from a tar file.
func (db *DB) Import(r io.Reader) error {
	return db.root.Import(r)
}

// Export exports the database to a tar file.
func (db *DB) Export(w io.Writer, exclude ...string) error {
	return db.root.Export(w, exclude...)
}

// ExportFile exports the entire database to a tar file.
// If the file has the gz suffix, it will be automatically compressed.
func (db *DB) ExportFile(fn string, exclude ...string) error {
	f, err := os.Create(fn)
	if err != nil {
		return err
	}

	var el oerrs.ErrorList
	el.PushIf(db.Export(f))
	el.PushIf(f.Close())
	return el.Err()
}

func (db *DB) CreateBucket(names ...string) (Bucket, error) {
	return db.root.CreateBucket(names...)
}

func (db *DB) Group(mws ...mw.Middleware) Bucket {
	return &group{db.root, mws}
}

func (db *DB) Close() error {
	db.lk.Close()
	return nil
}

func (db *DB) encodeKey(key string) string {
	if db.opts.PlainFileNames {
		checkValidKey(key)
		return key
	}
	return b64EncodeName(key)
}

func (db *DB) decodeKey(key string) (string, error) {
	if db.opts.PlainFileNames {
		checkValidKey(key)
		return key, nil
	}
	return b64DecodeName(key)
}

// isValidKey checks if the key can be a valid file path
// mostly based on https://en.wikipedia.org/wiki/Filename#Comparison_of_filename_limitations
// this function panics because this is a programmer error and the program shouldn't continue.
func checkValidKey(key string) {
	const badChars = "\x00\xff/\\:%?*|\"><"
	if key != "." && key != ".." && strings.ContainsAny(key, badChars) {
		log.Panicf("%q uses an invalid character (one of %q)", key, badChars)
	}
}

// Bucket is the interface for Bucket-like containers (buckets and groups)
type Bucket interface {
	Append(key string, r io.Reader, middlewares ...mw.Middleware) (err error)
	AppendFunc(key string, fn func(w io.Writer) error, middlewares ...mw.Middleware) (err error)
	Bucket(names ...string) Bucket
	Buckets(rev bool) (out []string)
	CreateBucket(names ...string) (Bucket, error)
	Delete(key string) (err error)
	Rename(key string, nBkt Bucket, nKey string) (err error)
	DeleteBucket(name string) (err error)
	ForEach(fn func(key string, value io.Reader) error, middlewares ...mw.Middleware) error
	ForEachReverse(fn func(key string, value io.Reader) error, middlewares ...mw.Middleware) error
	Get(key string, middlewares ...mw.Middleware) (_ io.ReadCloser, err error)
	GetAndDelete(key string, fn func(r io.Reader) error, middlewares ...mw.Middleware) (err error)
	GetAndRename(key string, nBkt Bucket, nKey string, overwrite bool, fn ReaderFn, mws ...mw.Middleware) (err error)
	Group(mws ...mw.Middleware) Bucket
	Keys(reverse bool) (out []string)
	Name() string
	NextID() *big.Int
	Path() string
	Put(key string, r io.Reader, middlewares ...mw.Middleware) (err error)
	PutFunc(key string, fn func(w io.Writer) error, middlewares ...mw.Middleware) (err error)
	PutTimed(key string, r io.Reader, expireAfter time.Duration, middlewares ...mw.Middleware) (err error)
	PutTimedFunc(key string, fn func(w io.Writer) error, expireAfter time.Duration, middlewares ...mw.Middleware) (err error)
	Import(r io.Reader) (err error)
	Export(w io.Writer, exclude ...string) (err error)
	Stat(key string) (fi os.FileInfo, err error)
	SetExtraData(fileKey, key string, val string) error
	GetExtraData(fileKey, key string) (out string)
	ExtraData(fileKey string) (out map[string]string)
	AllExtraData() (out map[string]map[string]string)
}
