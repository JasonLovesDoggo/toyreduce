package storage

import (
	"testing"
)

type testStruct struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestJSONStore(t *testing.T) {
	t.Run("PutAndGetJSON", func(t *testing.T) {
		backend := NewMemoryBackend()
		store := NewJSONStore(backend)
		defer store.Close()

		// Create bucket
		if err := store.CreateBucket([]byte("test")); err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		// Put JSON
		original := testStruct{Name: "test", Value: 42}
		if err := store.PutJSON([]byte("test"), []byte("key1"), original); err != nil {
			t.Fatalf("PutJSON failed: %v", err)
		}

		// Get JSON
		var got testStruct
		if err := store.GetJSON([]byte("test"), []byte("key1"), &got); err != nil {
			t.Fatalf("GetJSON failed: %v", err)
		}

		// Verify
		if got.Name != original.Name || got.Value != original.Value {
			t.Errorf("Got %+v, want %+v", got, original)
		}
	})

	t.Run("GetJSONNonExistent", func(t *testing.T) {
		backend := NewMemoryBackend()
		store := NewJSONStore(backend)
		defer store.Close()

		store.CreateBucket([]byte("test"))

		// Get non-existent key should not error
		var got testStruct
		if err := store.GetJSON([]byte("test"), []byte("nonexistent"), &got); err != nil {
			t.Errorf("GetJSON should not error for non-existent key: %v", err)
		}

		// Struct should be zero-valued
		if got.Name != "" || got.Value != 0 {
			t.Errorf("Got %+v, want zero value", got)
		}
	})

	t.Run("JSONArray", func(t *testing.T) {
		backend := NewMemoryBackend()
		store := NewJSONStore(backend)
		defer store.Close()

		store.CreateBucket([]byte("test"))

		// Store array
		original := []int{1, 2, 3, 4, 5}
		if err := store.PutJSON([]byte("test"), []byte("array"), original); err != nil {
			t.Fatalf("PutJSON failed: %v", err)
		}

		// Retrieve array
		var got []int
		if err := store.GetJSON([]byte("test"), []byte("array"), &got); err != nil {
			t.Fatalf("GetJSON failed: %v", err)
		}

		// Verify
		if len(got) != len(original) {
			t.Fatalf("Got length %d, want %d", len(got), len(original))
		}
		for i, v := range original {
			if got[i] != v {
				t.Errorf("Index %d: got %d, want %d", i, got[i], v)
			}
		}
	})

	t.Run("JSONMap", func(t *testing.T) {
		backend := NewMemoryBackend()
		store := NewJSONStore(backend)
		defer store.Close()

		store.CreateBucket([]byte("test"))

		// Store map
		original := map[string]int{
			"one":   1,
			"two":   2,
			"three": 3,
		}
		if err := store.PutJSON([]byte("test"), []byte("map"), original); err != nil {
			t.Fatalf("PutJSON failed: %v", err)
		}

		// Retrieve map
		var got map[string]int
		if err := store.GetJSON([]byte("test"), []byte("map"), &got); err != nil {
			t.Fatalf("GetJSON failed: %v", err)
		}

		// Verify
		if len(got) != len(original) {
			t.Fatalf("Got length %d, want %d", len(got), len(original))
		}
		for k, v := range original {
			if got[k] != v {
				t.Errorf("Key %s: got %d, want %d", k, got[k], v)
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		backend := NewMemoryBackend()
		store := NewJSONStore(backend)
		defer store.Close()

		store.CreateBucket([]byte("test"))

		// Put and delete
		original := testStruct{Name: "test", Value: 42}
		store.PutJSON([]byte("test"), []byte("key1"), original)

		if err := store.Delete([]byte("test"), []byte("key1")); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify deleted
		var got testStruct
		store.GetJSON([]byte("test"), []byte("key1"), &got)
		if got.Name != "" || got.Value != 0 {
			t.Errorf("Key should be deleted, got %+v", got)
		}
	})

	t.Run("Transaction", func(t *testing.T) {
		backend := NewMemoryBackend()
		store := NewJSONStore(backend)
		defer store.Close()

		// Use Update transaction
		err := store.Update(func(tx Transaction) error {
			if err := tx.CreateBucket([]byte("test")); err != nil {
				return err
			}
			b := tx.Bucket([]byte("test"))
			if b == nil {
				t.Fatal("Bucket should not be nil")
			}

			// Manually encode JSON and put
			data, err := EncodeJSON(testStruct{Name: "tx", Value: 99})
			if err != nil {
				return err
			}
			return b.Put([]byte("key1"), data)
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}

		// Use View transaction
		var got testStruct
		err = store.View(func(tx Transaction) error {
			b := tx.Bucket([]byte("test"))
			if b == nil {
				t.Fatal("Bucket should not be nil")
			}

			data := b.Get([]byte("key1"))
			if data == nil {
				t.Fatal("Data should not be nil")
			}
			return DecodeJSON(data, &got)
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}

		if got.Name != "tx" || got.Value != 99 {
			t.Errorf("Got %+v, want {tx 99}", got)
		}
	})
}

func TestEncodeDecodeJSON(t *testing.T) {
	t.Run("EncodeJSON", func(t *testing.T) {
		original := testStruct{Name: "test", Value: 42}
		data, err := EncodeJSON(original)
		if err != nil {
			t.Fatalf("EncodeJSON failed: %v", err)
		}
		if len(data) == 0 {
			t.Error("EncodeJSON returned empty data")
		}
	})

	t.Run("DecodeJSON", func(t *testing.T) {
		original := testStruct{Name: "test", Value: 42}
		data, _ := EncodeJSON(original)

		var got testStruct
		if err := DecodeJSON(data, &got); err != nil {
			t.Fatalf("DecodeJSON failed: %v", err)
		}

		if got.Name != original.Name || got.Value != original.Value {
			t.Errorf("Got %+v, want %+v", got, original)
		}
	})

	t.Run("DecodeJSONInvalidData", func(t *testing.T) {
		var got testStruct
		err := DecodeJSON([]byte("invalid json"), &got)
		if err == nil {
			t.Error("DecodeJSON should fail for invalid JSON")
		}
	})
}
