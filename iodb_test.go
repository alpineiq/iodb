//go:build !bolt
// +build !bolt

package iodb

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alpineiq/iodb/mw/common"
	"github.com/alpineiq/iodb/mw/compressors"
)

// TODO: clean this up, too much repeated code.

func TestConcurrentPutGet(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestConcurrentPutGet")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}

	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b, err := db.CreateBucket("TestConcurrentPutGet", "Test")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Put("license", strings.NewReader(data)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if n := db.NumOpenFiles(); n > 0 {
		t.Fatalf("unexpected number of open files, expected 0, got %v", n)
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := db.Bucket("TestConcurrentPutGet", "Test")
			if b == nil {
				t.Error("b == nil")
				return
			}
			rc, err := b.Get("license")
			if err != nil {
				t.Error(err)
				return
			}
			defer rc.Close()
			if h := hashString(rc); h != dataHash {
				t.Errorf("expected %s, got %s", dataHash, h)
				return
			}
		}()
	}
	wg.Wait()

	if n := db.NumOpenFiles(); n > 0 {
		t.Fatalf("unexpected number of open files, expected 0, got %v", n)
	}
	t.Logf("buckets: %v", db.Bucket().Buckets(false))
}

func TestMiddlewareGroups(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestMiddlewareGroups")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}

	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// yes double compression is pointless but we're just testing the middleware.
	g := db.Group(compressors.NewFlate(9), compressors.NewGzip(9), common.NewBase64())

	b, err := g.CreateBucket("TestMiddlewareGroups")
	if err != nil {
		t.Fatal(err)
	}

	if err = b.Put("license", strings.NewReader(data)); err != nil {
		t.Fatalf("%v", err)
	}

	rc, err := b.Get("license")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	if h := hashString(rc); h != dataHash {
		t.Fatalf("expected %s, got %s", dataHash, h)
	}
}

func TestChainBucket(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestChainBucket")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}
	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b1, err := db.CreateBucket("b1", "b2", "b3")
	if err != nil {
		t.Fatal(err)
	}
	b2 := db.Bucket("b1").Bucket("b2").Bucket("b3")

	if b1 != b2 { // both should be the very same pointer otherwise something is very wrong.
		t.Fatal("b1 != b2")
	}

	t.Logf("path: %s, name: %v", b1.Path(), b1.Name())
}

func TestBugCantListBucketsAndKeys(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestChainBucket")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}
	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err = db.CreateBucket("b1", "b2"); err != nil {
		t.Fatal(err)
	}

	b := db.Bucket() // root bucket, should have "b1" in it
	if len(b.Buckets(false)) == 0 {
		t.Fatal("len(root.Buckets(false)) == 0")
	}
	b = b.Bucket("b1")
	if len(b.Buckets(false)) == 0 {
		t.Fatal("len(b1.Buckets(false)) == 0")
	}
	if b = b.Bucket("b2"); b == nil {
		t.Fatal("b2 == nil")
	}
	if err = b.Put("test", strings.NewReader("test")); err != nil {
		t.Fatal(err)
	}

	if len(b.Keys(false)) == 0 {
		t.Fatal("len(b2.Keys(false)) == 0")
	}

	if b.Bucket("doesn't exist") != nil {
		t.Fatal("should have been nil")
	}

	t.Logf("Bucket (%s): %q %q", b.Name(), b.Buckets(true), b.Keys(true))
}

func TestTimedKey(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestTimedBucket")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}
	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b, err := db.CreateBucket("TestTimed")
	if err != nil {
		t.Fatal(err)
	}

	if err = b.PutTimed("license", strings.NewReader(data), time.Second/4); err != nil {
		t.Fatalf("%v", err)
	}

	rc, err := b.Get("license")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	if h := hashString(rc); h != dataHash {
		t.Fatalf("expected %s, got %s", dataHash, h)
	}
	time.Sleep(time.Second / 2)

	if _, err = b.Get("license"); err != os.ErrNotExist {
		t.Fatal("file didn't get deleted")
	}
}

func TestTimedKeyBucketReload(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestTimedBucket")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}
	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}

	b, err := db.CreateBucket("TestTimed")
	if err != nil {
		t.Fatal(err)
	}

	if err = b.PutTimed("license", strings.NewReader(data), time.Second/4); err != nil {
		t.Fatalf("%v", err)
	}
	db.Close()

	time.Sleep(time.Second / 2)

	if db, err = New(tmpDir, nil); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err = db.Bucket("TestTimed").Get("license"); err != os.ErrNotExist {
		t.Fatal("file didn't get deleted")
	}
}

func TestAppend(t *testing.T) {
	const nums = "0123456789"
	tmpDir, err := ioutil.TempDir("", "iodb-TestAppend")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}
	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b, err := db.CreateBucket("TestAppend")
	if err != nil {
		t.Fatal(err)
	}

	if err = b.Put("f", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}

	for _, c := range nums {
		if err = b.Append("f", strings.NewReader(string(c))); err != nil {
			t.Fatal(err)
		}
	}

	rc, err := b.Get("f")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if s := readString(rc); s != "x"+nums {
		t.Fatalf("expected %q, got %q", nums, s)
	}
}

func TestGetAndDelete(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestGetDelete")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}
	db, err := New(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b, err := db.CreateBucket("TestGetDelete")
	if err != nil {
		t.Fatal(err)
	}

	if err = b.Put("license", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err = b.GetAndDelete("license", func(r io.Reader) error {
		if h := hashString(r); h != dataHash {
			return fmt.Errorf("expected %s, got %s", dataHash, h)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Get("license"); err != os.ErrNotExist {
		t.Fatal("found license, but we shouldn't have")
	}
}

func TestGetAndRename(t *testing.T) {
	var (
		tmpDir string
		db     *DB
		b, nb  Bucket
		err    error
	)

	if tmpDir, err = ioutil.TempDir("", "iodb-TestGetRename"); err != nil {
		t.Fatal(err)
	}

	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}

	if db, err = New(tmpDir, &Options{
		PlainFileNames: true,
	}); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if b, err = db.CreateBucket("TestOrig"); err != nil {
		t.Fatal(err)
	}

	if nb, err = db.CreateBucket("TestNew"); err != nil {
		t.Fatal(err)
	}

	if err = b.Put("license", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if err = b.GetAndRename("license", nb, "license", false, func(r io.Reader) error {
		if h := hashString(r); h != dataHash {
			return fmt.Errorf("expected %s, got %s", dataHash, h)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err = b.Get("license"); err != os.ErrNotExist {
		t.Fatal("found license in original bucket, but we shouldn't have")
	}

	var rc io.ReadCloser
	if rc, err = nb.Get("license"); err != nil {
		t.Fatal("didn't find licence in the new bucket")
	}
	rc.Close()
}

func TestRename(t *testing.T) {
	var (
		tmpDir string
		db     *DB
		b, nb  Bucket
		err    error
	)

	if tmpDir, err = ioutil.TempDir("", "iodb-TestGetRename"); err != nil {
		t.Fatal(err)
	}

	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}

	if db, err = New(tmpDir, &Options{
		PlainFileNames: true,
	}); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if b, err = db.CreateBucket("TestOrig"); err != nil {
		t.Fatal(err)
	}

	if nb, err = db.CreateBucket("TestNew"); err != nil {
		t.Fatal(err)
	}

	if err = b.Put("license", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if err = b.Rename("license", nb, "license"); err != nil {
		t.Fatal(err)
	}

	if _, err = b.Get("license"); err != os.ErrNotExist {
		t.Fatal("found license in original bucket, but we shouldn't have")
	}

	var rc io.ReadCloser
	if rc, err = nb.Get("license"); err != nil {
		t.Fatal("didn't find licence in the new bucket")
	}
	rc.Close()

	if err = nb.Rename("license", nb, "license.archived"); err != nil {
		t.Fatal(err)
	}

	if _, err = nb.Get("license"); err != os.ErrNotExist {
		t.Fatal("found license in new bucket, but we shouldn't have")
	}

	if rc, err = nb.Get("license.archived"); err != nil {
		t.Fatal("didn't find licence in the new bucket")
	}
	rc.Close()
}

func TestExport(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "iodb-TestExport")
	if err != nil {
		t.Fatal(err)
	}
	if !keepTmp {
		defer os.RemoveAll(tmpDir)
	}

	// create a new empty database
	db, err := New(tmpDir, &Options{PlainFileNames: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// put license in the root bucket
	if err = db.Bucket().Put("license", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// create a new child bucket
	b, err := db.CreateBucket("Child Bucket")
	if err != nil {
		t.Fatal(err)
	}

	// put license in the child bucket
	if err = b.Put("license", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// create a child bucket under the child bucket, we have grandchildren now fam!
	b, err = b.CreateBucket("Child Child Bucket")
	if err != nil {
		t.Fatal(err)
	}

	// there's a pattern here somewhere!
	if err = b.Put("license", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	// export the database to an in-memory buffer as a tar
	if err = db.root.Export(&buf); err != nil {
		t.Fatal(err)
	}

	r := bytes.NewReader(buf.Bytes())
	tr := tar.NewReader(r)

	// read all the files back and make sure they are readable and correct
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Checking: %s (%d bytes)", hdr.Name, hdr.Size)
		if h := hashString(tr); h != dataHash {
			t.Fatalf("expected %s, got %s", dataHash, h)
		}
	}
}

func TestStat(t *testing.T) {
	var (
		db     *DB
		bkt    Bucket
		fi     os.FileInfo
		tmpDir string
		err    error
	)

	if tmpDir, err = ioutil.TempDir("", "iodb-TestStat"); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll(tmpDir)

	if db, err = New(tmpDir, nil); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if bkt, err = db.CreateBucket("TestStat"); err != nil {
		t.Fatal(err)
	}

	if err = bkt.Put("license", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// This should work
	if fi, err = bkt.Stat("license"); err != nil {
		t.Fatal(err)
	}

	if fi.Size() == 0 {
		t.Fatal("file size returns 0 when it should have a value")
	}

	if err = bkt.SetExtraData("license", "wut?", "42"); err != nil {
		t.Fatalf("SetExtraData: %v", err)
	}

	if v := bkt.GetExtraData("license", "wut?"); v != "42" {
		t.Fatalf("the meaning of life, the universe, and everything is gone :(")
	}

	t.Logf("%q", bkt.AllExtraData())

	// This should not work
	if _, err = bkt.Stat("nolicense"); err == nil {
		t.Fatal("file info is being returned when it should be nil")
	}
}
