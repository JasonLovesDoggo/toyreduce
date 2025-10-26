package maxvalue

import (
	"strconv"
	"strings"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// MaxValueWorker finds the maximum numeric value for each key.
// Input format: "key:value" per line (e.g., "temperature:72.5")
type MaxValueWorker struct{}

// Map extracts key-value pairs and emits them
func (w MaxValueWorker) Map(chunk []string, emit toyreduce.Emitter) error {
	for _, line := range chunk {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			emit(toyreduce.KeyValue{Key: parts[0], Value: parts[1]})
		}
	}
	return nil
}

// Combine finds the local maximum for each key (runs after Map)
func (w MaxValueWorker) Combine(key string, values []string, emit toyreduce.Emitter) error {
	if len(values) == 0 {
		return nil
	}

	maxVal := parseFloat(values[0])
	for _, v := range values[1:] {
		if val := parseFloat(v); val > maxVal {
			maxVal = val
		}
	}

	emit(toyreduce.KeyValue{Key: key, Value: strconv.FormatFloat(maxVal, 'f', -1, 64)})
	return nil
}

// Reduce finds the global maximum for each key across all combined results
func (w MaxValueWorker) Reduce(key string, values []string, emit toyreduce.Emitter) error {
	// Same logic as Combine - finding maximum is associative
	return w.Combine(key, values, emit)
}

func (w MaxValueWorker) Description() string {
	return "Finds the maximum numeric value for each key (format: key:value)"
}

func parseFloat(s string) float64 {
	val, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return val
}
