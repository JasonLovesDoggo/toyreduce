package wordcount

import (
	"strconv"
	"strings"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// WordCountWorker implements toyreduce.Worker
type WordCountWorker struct{}

// Map splits each line into words and emits (word, "1") pairs.
func (w WordCountWorker) Map(chunk []string, emit toyreduce.Emitter) error {
	for _, line := range chunk {
		for _, word := range strings.Fields(line) {
			emit(toyreduce.KeyValue{Key: word, Value: "1"})
		}
	}

	return nil
}

// Reduce receives all values for a word and emits (word, count)
func (w WordCountWorker) Reduce(key string, values []string, emit toyreduce.Emitter) error {
	emit(toyreduce.KeyValue{Key: key, Value: strconv.Itoa(len(values))})
	return nil
}

func (w WordCountWorker) Description() string {
	return "A simple word count worker that counts occurrences of each word"
}
