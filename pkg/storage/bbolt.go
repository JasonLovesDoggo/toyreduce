package storage

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// BboltBackend implements Backend using bbolt (formerly bolt)
type BboltBackend struct {
	db *bolt.DB
}

// NewBboltBackend creates a new bbolt-backed storage backend
func NewBboltBackend(dbPath string) (*BboltBackend, error) {
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

	return &BboltBackend{db: db}, nil
}

// CreateBucket creates a new bucket
func (b *BboltBackend) CreateBucket(name []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(name)
		return err
	})
}

// DeleteBucket deletes a bucket
func (b *BboltBackend) DeleteBucket(name []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(name)
		if err == bolt.ErrBucketNotFound {
			return nil // Idempotent
		}
		return err
	})
}

// BucketExists checks if a bucket exists
func (b *BboltBackend) BucketExists(name []byte) (bool, error) {
	exists := false
	err := b.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket(name) != nil
		return nil
	})
	return exists, err
}

// Put stores a key-value pair in a bucket
func (b *BboltBackend) Put(bucket, key, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucket)
		if bkt == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		return bkt.Put(key, value)
	})
}

// Get retrieves a value from a bucket
func (b *BboltBackend) Get(bucket, key []byte) ([]byte, error) {
	var value []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucket)
		if bkt == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		v := bkt.Get(key)
		if v != nil {
			// Copy the value since it's only valid during the transaction
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	return value, err
}

// Delete removes a key from a bucket
func (b *BboltBackend) Delete(bucket, key []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucket)
		if bkt == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		return bkt.Delete(key)
	})
}

// ForEach iterates over all key-value pairs in a bucket
func (b *BboltBackend) ForEach(bucket []byte, fn func(k, v []byte) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucket)
		if bkt == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		return bkt.ForEach(fn)
	})
}

// Update executes a function within a read-write transaction
func (b *BboltBackend) Update(fn func(tx Transaction) error) error {
	return b.db.Update(func(boltTx *bolt.Tx) error {
		return fn(&bboltTransaction{tx: boltTx})
	})
}

// View executes a function within a read-only transaction
func (b *BboltBackend) View(fn func(tx Transaction) error) error {
	return b.db.View(func(boltTx *bolt.Tx) error {
		return fn(&bboltTransaction{tx: boltTx})
	})
}

// Close closes the database
func (b *BboltBackend) Close() error {
	return b.db.Close()
}

// bboltTransaction wraps a bolt transaction
type bboltTransaction struct {
	tx *bolt.Tx
}

func (t *bboltTransaction) CreateBucket(name []byte) error {
	_, err := t.tx.CreateBucketIfNotExists(name)
	return err
}

func (t *bboltTransaction) DeleteBucket(name []byte) error {
	err := t.tx.DeleteBucket(name)
	if err == bolt.ErrBucketNotFound {
		return nil // Idempotent
	}
	return err
}

func (t *bboltTransaction) Bucket(name []byte) Bucket {
	bkt := t.tx.Bucket(name)
	if bkt == nil {
		return nil
	}
	return &bboltBucket{bucket: bkt}
}

func (t *bboltTransaction) ForEachBucket(fn func(name []byte) error) error {
	return t.tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		return fn(name)
	})
}

// bboltBucket wraps a bolt bucket
type bboltBucket struct {
	bucket *bolt.Bucket
}

func (b *bboltBucket) Put(key, value []byte) error {
	return b.bucket.Put(key, value)
}

func (b *bboltBucket) Get(key []byte) []byte {
	return b.bucket.Get(key)
}

func (b *bboltBucket) Delete(key []byte) error {
	return b.bucket.Delete(key)
}

func (b *bboltBucket) ForEach(fn func(k, v []byte) error) error {
	return b.bucket.ForEach(fn)
}
