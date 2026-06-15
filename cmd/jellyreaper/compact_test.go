package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	bboltlib "go.etcd.io/bbolt"
)

// openTestDB opens a bbolt database at path (creating buckets as needed).
func openTestDB(t *testing.T, path string, readOnly bool) *bboltlib.DB {
	t.Helper()
	opts := &bboltlib.Options{Timeout: time.Second, ReadOnly: readOnly}
	db, err := bboltlib.Open(path, 0o600, opts)
	if err != nil {
		t.Fatalf("open db %s: %v", path, err)
	}
	return db
}

// TestRunCompact_Basic verifies that runCompact produces a dst that:
//   - opens without error
//   - is <= src size (free-page reclamation)
//   - round-trips a known key
func TestRunCompact_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "src.db")
	dstPath := filepath.Join(tmpDir, "dst.db")

	// ── Build a src database with data + freed space ─────────────────────────
	src := openTestDB(t, srcPath, false)

	// Write a bunch of data and then delete it to create free pages.
	const bucketName = "testbucket"
	err := src.Update(func(tx *bboltlib.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		// Write 100 medium-sized values.
		for i := 0; i < 100; i++ {
			key := []byte(string(rune('a'+i%26)) + "-key")
			val := make([]byte, 4096)
			for j := range val {
				val[j] = byte(i)
			}
			if err := b.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("write data: %v", err)
	}

	// Delete most of the data to create free pages.
	err = src.Update(func(tx *bboltlib.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		for i := 0; i < 95; i++ {
			key := []byte(string(rune('a'+i%26)) + "-key")
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("delete data: %v", err)
	}

	// Write a sentinel key we can verify survives compaction.
	const sentinelKey = "sentinel"
	const sentinelVal = "hello-compact"
	err = src.Update(func(tx *bboltlib.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		return b.Put([]byte(sentinelKey), []byte(sentinelVal))
	})
	if err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	// ── Run compact ───────────────────────────────────────────────────────────
	if err := runCompact(srcPath, dstPath); err != nil {
		t.Fatalf("runCompact: %v", err)
	}

	// ── Verify dst ────────────────────────────────────────────────────────────
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		t.Fatalf("stat src: %v", err)
	}
	dstInfo, err := os.Stat(dstPath)
	if err != nil {
		t.Fatalf("stat dst: %v", err)
	}

	if dstInfo.Size() > srcInfo.Size() {
		t.Errorf("expected dst (%d bytes) <= src (%d bytes) after compaction", dstInfo.Size(), srcInfo.Size())
	}

	// Verify dst opens successfully and the sentinel key round-trips.
	dst := openTestDB(t, dstPath, false)
	defer dst.Close()

	var got string
	err = dst.View(func(tx *bboltlib.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			t.Error("bucket missing in dst after compact")
			return nil
		}
		v := b.Get([]byte(sentinelKey))
		if v == nil {
			t.Error("sentinel key missing in dst after compact")
			return nil
		}
		got = string(v)
		return nil
	})
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if got != sentinelVal {
		t.Errorf("sentinel round-trip: want %q, got %q", sentinelVal, got)
	}
}

// TestRunCompact_MissingSrc verifies an error is returned when src doesn't exist.
func TestRunCompact_MissingSrc(t *testing.T) {
	tmpDir := t.TempDir()
	err := runCompact(filepath.Join(tmpDir, "nosuchfile.db"), filepath.Join(tmpDir, "dst.db"))
	if err == nil {
		t.Fatal("expected error for missing src, got nil")
	}
}
