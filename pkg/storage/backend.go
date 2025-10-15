package storage

// Backend defines a generic key-value storage interface with bucket support.
// All operations work with raw []byte for maximum flexibility.
// Implementations can choose their serialization format (JSON, protobuf, msgpack, etc.)
type Backend interface {
	// Bucket operations
	CreateBucket(name []byte) error
	DeleteBucket(name []byte) error
	BucketExists(name []byte) (bool, error)

	// KV operations within buckets
	Put(bucket, key, value []byte) error
	Get(bucket, key []byte) ([]byte, error)
	Delete(bucket, key []byte) error

	// Iteration
	ForEach(bucket []byte, fn func(k, v []byte) error) error

	// Batch operations for transactions
	Update(fn func(tx Transaction) error) error
	View(fn func(tx Transaction) error) error

	// Lifecycle
	Close() error
}

// String-based convenience methods to avoid constant []byte conversions

// PutString is a convenience wrapper that converts string keys to []byte
func PutString(b Backend, bucket []byte, key string, value []byte) error {
	return b.Put(bucket, []byte(key), value)
}

// GetString is a convenience wrapper that converts string keys to []byte
func GetString(b Backend, bucket []byte, key string) ([]byte, error) {
	return b.Get(bucket, []byte(key))
}

// DeleteString is a convenience wrapper that converts string keys to []byte
func DeleteString(b Backend, bucket []byte, key string) error {
	return b.Delete(bucket, []byte(key))
}

// Transaction provides transactional access to the backend
type Transaction interface {
	// Bucket operations
	CreateBucket(name []byte) error
	DeleteBucket(name []byte) error
	Bucket(name []byte) Bucket

	// ForEachBucket iterates over all bucket names
	ForEachBucket(fn func(name []byte) error) error
}

// Bucket provides access to a single bucket within a transaction
type Bucket interface {
	Put(key, value []byte) error
	Get(key []byte) []byte
	Delete(key []byte) error
	ForEach(fn func(k, v []byte) error) error
}
