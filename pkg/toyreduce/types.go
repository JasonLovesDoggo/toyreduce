package toyreduce

type KeyValue struct {
	Key   string
	Value string
}

type Worker interface {
	Map(chunk []string, emit Emitter) error
	Reduce(key string, values []string, emit Emitter) error
	Description() string
}

type Emitter func(KeyValue)
