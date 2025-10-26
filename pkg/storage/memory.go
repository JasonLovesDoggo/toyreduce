package storage

import (
	"fmt"
	"sync"
)

// MemoryBackend implements Backend using in-memory maps (not persistent)
type MemoryBackend struct {
	buckets map[string]map[string][]byte
	mu      sync.RWMutex
}

// NewMemoryBackend creates a new in-memory storage backend
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		buckets: make(map[string]map[string][]byte),
	}
}

// CreateBucket creates a new bucket
func (m *MemoryBackend) CreateBucket(name []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	nameStr := string(name)
	if _, exists := m.buckets[nameStr]; !exists {
		m.buckets[nameStr] = make(map[string][]byte)
	}

	return nil
}

// DeleteBucket deletes a bucket
func (m *MemoryBackend) DeleteBucket(name []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.buckets, string(name))

	return nil
}

// BucketExists checks if a bucket exists
func (m *MemoryBackend) BucketExists(name []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.buckets[string(name)]

	return exists, nil
}

// Put stores a key-value pair in a bucket
func (m *MemoryBackend) Put(bucket, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bucketStr := string(bucket)

	bkt, exists := m.buckets[bucketStr]
	if !exists {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	// Copy value to prevent external modifications
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	bkt[string(key)] = valueCopy

	return nil
}

// Get retrieves a value from a bucket
func (m *MemoryBackend) Get(bucket, key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bucketStr := string(bucket)

	bkt, exists := m.buckets[bucketStr]
	if !exists {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	value, exists := bkt[string(key)]
	if !exists {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	return valueCopy, nil
}

// Delete removes a key from a bucket
func (m *MemoryBackend) Delete(bucket, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bucketStr := string(bucket)

	bkt, exists := m.buckets[bucketStr]
	if !exists {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	delete(bkt, string(key))

	return nil
}

// ForEach iterates over all key-value pairs in a bucket
func (m *MemoryBackend) ForEach(bucket []byte, fn func(k, v []byte) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bucketStr := string(bucket)

	bkt, exists := m.buckets[bucketStr]
	if !exists {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	for k, v := range bkt {
		if err := fn([]byte(k), v); err != nil {
			return err
		}
	}

	return nil
}

// Update executes a function within a "transaction" (memory backend doesn't need real transactions)
func (m *MemoryBackend) Update(fn func(tx Transaction) error) error {
	return fn(&memoryTransaction{backend: m})
}

// View executes a function within a read-only "transaction"
func (m *MemoryBackend) View(fn func(tx Transaction) error) error {
	return fn(&memoryTransaction{backend: m})
}

// Close is a no-op for memory backend
func (m *MemoryBackend) Close() error {
	return nil
}

// memoryTransaction wraps the memory backend for transaction interface
type memoryTransaction struct {
	backend *MemoryBackend
}

func (t *memoryTransaction) CreateBucket(name []byte) error {
	return t.backend.CreateBucket(name)
}

func (t *memoryTransaction) DeleteBucket(name []byte) error {
	return t.backend.DeleteBucket(name)
}

func (t *memoryTransaction) Bucket(name []byte) Bucket {
	t.backend.mu.RLock()
	defer t.backend.mu.RUnlock()

	bucketStr := string(name)
	if _, exists := t.backend.buckets[bucketStr]; !exists {
		return nil
	}

	return &memoryBucket{
		backend:    t.backend,
		bucketName: bucketStr,
	}
}

func (t *memoryTransaction) ForEachBucket(fn func(name []byte) error) error {
	t.backend.mu.RLock()
	defer t.backend.mu.RUnlock()

	for name := range t.backend.buckets {
		if err := fn([]byte(name)); err != nil {
			return err
		}
	}

	return nil
}

// memoryBucket provides bucket operations for memory backend
type memoryBucket struct {
	backend    *MemoryBackend
	bucketName string
}

func (b *memoryBucket) Put(key, value []byte) error {
	return b.backend.Put([]byte(b.bucketName), key, value)
}

func (b *memoryBucket) Get(key []byte) []byte {
	value, _ := b.backend.Get([]byte(b.bucketName), key)
	return value
}

func (b *memoryBucket) Delete(key []byte) error {
	return b.backend.Delete([]byte(b.bucketName), key)
}

func (b *memoryBucket) ForEach(fn func(k, v []byte) error) error {
	return b.backend.ForEach([]byte(b.bucketName), fn)
}
