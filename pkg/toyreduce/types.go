package toyreduce

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Worker interface {
	Map(chunk []string, emit Emitter) error
	Reduce(key string, values []string, emit Emitter) error
	Description() string
}

type Emitter func(KeyValue)

// CombinableWorker is an optional interface that Workers can implement
// to provide custom combining behavior or opt-out of combining entirely.
//
// By default, all workers get combining enabled using their Reduce() function.
// Workers can:
// 1. Implement Combine() for custom combine logic
// 2. Implement DisableCombiner() returning true to opt-out
type CombinableWorker interface {
	Worker
	// Combine is called after Map to pre-aggregate values locally.
	// If not implemented, the worker's Reduce() function is used by default.
	Combine(key string, values []string, emit Emitter) error
}

// DisableCombinerCheck is an optional interface to opt-out of combining.
type DisableCombinerCheck interface {
	// DisableCombiner returns true to skip the combine phase entirely.
	DisableCombiner() bool
}
