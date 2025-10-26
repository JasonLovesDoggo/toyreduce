package storage

import (
	"encoding/json"
	"fmt"
)

// JSONStore wraps a Backend and provides JSON serialization convenience methods
type JSONStore struct {
	backend Backend
}

// NewJSONStore creates a new JSON store wrapper around a backend
func NewJSONStore(backend Backend) *JSONStore {
	return &JSONStore{backend: backend}
}

// Backend returns the underlying backend
func (j *JSONStore) Backend() Backend {
	return j.backend
}

// PutJSON stores a JSON-encoded value in a bucket
func (j *JSONStore) PutJSON(bucket, key []byte, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return j.backend.Put(bucket, key, data)
}

// GetJSON retrieves and JSON-decodes a value from a bucket
func (j *JSONStore) GetJSON(bucket, key []byte, v interface{}) error {
	data, err := j.backend.Get(bucket, key)
	if err != nil {
		return err
	}

	if data == nil {
		return nil // Key not found, don't decode
	}

	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

// ForEachJSON iterates over all key-value pairs in a bucket, decoding values as JSON
func (j *JSONStore) ForEachJSON(bucket []byte, fn func(k []byte, v []byte) error) error {
	return j.backend.ForEach(bucket, fn)
}

// CreateBucket creates a new bucket
func (j *JSONStore) CreateBucket(name []byte) error {
	return j.backend.CreateBucket(name)
}

// DeleteBucket deletes a bucket
func (j *JSONStore) DeleteBucket(name []byte) error {
	return j.backend.DeleteBucket(name)
}

// Delete removes a key from a bucket
func (j *JSONStore) Delete(bucket, key []byte) error {
	return j.backend.Delete(bucket, key)
}

// Update executes a function within a transaction
func (j *JSONStore) Update(fn func(tx Transaction) error) error {
	return j.backend.Update(fn)
}

// View executes a function within a read-only transaction
func (j *JSONStore) View(fn func(tx Transaction) error) error {
	return j.backend.View(fn)
}

// Close closes the underlying backend
func (j *JSONStore) Close() error {
	return j.backend.Close()
}

// Helper functions for direct JSON encoding/decoding (exported for convenience)

// EncodeJSON marshals a value to JSON bytes
func EncodeJSON(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode JSON: %w", err)
	}

	return data, nil
}

// DecodeJSON unmarshals JSON bytes to a value
func DecodeJSON(data []byte, v interface{}) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}
