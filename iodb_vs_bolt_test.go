// +build bolt

package iodb

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/boltdb/bolt"
)

var (
	db  *DB
	bdb *bolt.DB

	keyCounter int64

	data512, hash512 = getData(512)
	data12k, hash12k = getData(12 * 1024)
	data96k, hash96k = getData(96 * 1024)
	data1m, hash1m   = getData(1 * 1024 * 1024)
	data25m, hash25m = getData(25 * 1024 * 1024)
)

func BenchmarkIODB512RW(b *testing.B) {
	benchIODB(b, data512, hash512)
}

func BenchmarkIODB12kRW(b *testing.B) {
	benchIODB(b, data12k, hash12k)
}

func BenchmarkIODB96kRW(b *testing.B) {
	benchIODB(b, data96k, hash96k)
}

func BenchmarkIODB1mRW(b *testing.B) {
	benchIODB(b, data1m, hash1m)
}

func BenchmarkIODB25mRW(b *testing.B) {
	benchIODB(b, data25m, hash25m)
}

func benchIODB(b *testing.B, data []byte, hash string) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := getKey()
			bucket := db.Bucket("benchmark")
			if err := bucket.Put(key, bytes.NewReader(data)); err != nil {
				b.Fatal(err)
			}
			rc, err := bucket.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			if h := hashString(rc); h != hash {
				b.Fatalf("expected %s, got %s", hash, h)
			}
			rc.Close()
		}
	})
}

func BenchmarkBolt512RW(b *testing.B) {
	benchBolt(b, data512, hash512)
}

func BenchmarkBolt12kRW(b *testing.B) {
	benchBolt(b, data12k, hash12k)
}

func BenchmarkBolt96kRW(b *testing.B) {
	benchBolt(b, data96k, hash96k)
}

func BenchmarkBolt1mRW(b *testing.B) {
	benchBolt(b, data1m, hash1m)
}

func BenchmarkBolt25mRW(b *testing.B) {
	benchBolt(b, data25m, hash25m)
}

func benchBolt(b *testing.B, data []byte, hash string) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := getKey()
			if err := bdb.Update(func(tx *bolt.Tx) error {
				return boltPutReader(tx, "benchmark", key, bytes.NewReader(data))
			}); err != nil {
				b.Fatal(err)
			}

			if err := bdb.View(func(tx *bolt.Tx) error {
				rc := boltGetReader(tx, "benchmark", key)
				if h := hashString(rc); h != hash {
					b.Fatalf("expected %s, got %s", hash, h)
				}
				return rc.Close()
			}); err != nil {
				b.Fatal(err)
			}
		}

	})
}

func boltPutReader(tx *bolt.Tx, bucket, key string, r io.Reader) error {
	val, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	return tx.Bucket([]byte(bucket)).Put([]byte(key), val)
}

func boltGetReader(tx *bolt.Tx, bucket, key string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader(tx.Bucket([]byte(bucket)).Get([]byte(key))))
}

func TestMain(m *testing.M) {
	st := 1
	defer func() {
		os.Exit(st)
	}()

	tmpDir, err := ioutil.TempDir("", "iodb-bench")
	if err != nil {
		panic(err)
	}
	log.Println("iodb tmpDir:", tmpDir)
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}

	if db, err = New(tmpDir, nil); err != nil {
		panic(err)
	}
	db.CreateBucket("benchmark")

	tmpDir, err = ioutil.TempDir("", "bolt-bench")
	if err != nil {
		panic(err)
	}

	log.Println("bolt tmpDir:", tmpDir)

	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}

	if bdb, err = bolt.Open(filepath.Join(tmpDir, "bolt.db"), 0644, nil); err != nil {
		panic(err)
	}
	bdb.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("benchmark"))
		return nil
	})

	st = m.Run()
}

const keyLimit = 100

func getKey() string { // not really atomically correct but good enough for our test case
	v := atomic.AddInt64(&keyCounter, 1)
	if v == keyLimit {
		atomic.StoreInt64(&keyCounter, 0)
	}
	return strconv.Itoa(int(v))
}
