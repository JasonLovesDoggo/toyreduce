package storage

import (
	"bytes"
	"testing"
)

// backendTestSuite runs a comprehensive test suite against any Backend implementation
func backendTestSuite(t *testing.T, newBackend func() (Backend, func(), error)) {
	t.Run("CreateBucket", func(t *testing.T) {
		backend, cleanup, err := newBackend()
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer cleanup()

		if err := backend.CreateBucket([]byte("test")); err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		exists, err := backend.BucketExists([]byte("test"))
		if err != nil {
			t.Fatalf("BucketExists failed: %v", err)
		}
		if !exists {
			t.Error("Bucket should exist after creation")
		}

		// Idempotent
		if err := backend.CreateBucket([]byte("test")); err != nil {
			t.Errorf("CreateBucket should be idempotent: %v", err)
		}
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		backend, cleanup, err := newBackend()
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer cleanup()

		backend.CreateBucket([]byte("test"))
		if err := backend.DeleteBucket([]byte("test")); err != nil {
			t.Fatalf("DeleteBucket failed: %v", err)
		}

		exists, _ := backend.BucketExists([]byte("test"))
		if exists {
			t.Error("Bucket should not exist after deletion")
		}

		// Idempotent
		if err := backend.DeleteBucket([]byte("test")); err != nil {
			t.Errorf("DeleteBucket should be idempotent: %v", err)
		}
	})

	t.Run("PutAndGet", func(t *testing.T) {
		backend, cleanup, err := newBackend()
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer cleanup()

		backend.CreateBucket([]byte("test"))

		key := []byte("key1")
		value := []byte("value1")
		if err := backend.Put([]byte("test"), key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		got, err := backend.Get([]byte("test"), key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(got, value) {
			t.Errorf("Get returned %s, want %s", got, value)
		}

		// Non-existent key
		got, err = backend.Get([]byte("test"), []byte("nonexistent"))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got != nil {
			t.Errorf("Get should return nil for non-existent key, got %s", got)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		backend, cleanup, err := newBackend()
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer cleanup()

		backend.CreateBucket([]byte("test"))
		key := []byte("key1")
		backend.Put([]byte("test"), key, []byte("value1"))

		if err := backend.Delete([]byte("test"), key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		got, _ := backend.Get([]byte("test"), key)
		if got != nil {
			t.Error("Key should not exist after deletion")
		}
	})

	t.Run("ForEach", func(t *testing.T) {
		backend, cleanup, err := newBackend()
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer cleanup()

		backend.CreateBucket([]byte("test"))

		expected := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}
		for k, v := range expected {
			backend.Put([]byte("test"), []byte(k), []byte(v))
		}

		collected := make(map[string]string)
		err = backend.ForEach([]byte("test"), func(k, v []byte) error {
			collected[string(k)] = string(v)
			return nil
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}

		if len(collected) != len(expected) {
			t.Errorf("ForEach collected %d items, want %d", len(collected), len(expected))
		}
		for k, v := range expected {
			if collected[k] != v {
				t.Errorf("ForEach: key %s = %s, want %s", k, collected[k], v)
			}
		}
	})

	t.Run("Transactions", func(t *testing.T) {
		backend, cleanup, err := newBackend()
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer cleanup()

		err = backend.Update(func(tx Transaction) error {
			if err := tx.CreateBucket([]byte("test")); err != nil {
				return err
			}
			b := tx.Bucket([]byte("test"))
			if b == nil {
				t.Fatal("Bucket should not be nil")
			}
			return b.Put([]byte("key1"), []byte("value1"))
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}

		var gotValue []byte
		err = backend.View(func(tx Transaction) error {
			b := tx.Bucket([]byte("test"))
			if b == nil {
				t.Fatal("Bucket should not be nil")
			}
			gotValue = b.Get([]byte("key1"))
			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
		if !bytes.Equal(gotValue, []byte("value1")) {
			t.Errorf("Got %s, want value1", gotValue)
		}
	})

	t.Run("ForEachBucket", func(t *testing.T) {
		backend, cleanup, err := newBackend()
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer cleanup()

		buckets := []string{"bucket1", "bucket2", "bucket3"}
		for _, name := range buckets {
			backend.CreateBucket([]byte(name))
		}

		var collected []string
		err = backend.View(func(tx Transaction) error {
			return tx.ForEachBucket(func(name []byte) error {
				collected = append(collected, string(name))
				return nil
			})
		})
		if err != nil {
			t.Fatalf("ForEachBucket failed: %v", err)
		}

		if len(collected) != len(buckets) {
			t.Errorf("ForEachBucket found %d buckets, want %d", len(collected), len(buckets))
		}
	})
}
