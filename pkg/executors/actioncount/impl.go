package actioncount

import (
	"context"
	"strconv"
	"strings"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// ActionCountWorker implements toyreduce.Worker
type ActionCountWorker struct{}

// Map extracts the action (word after "did") from each line and emits (action, "1")
func (w ActionCountWorker) Map(ctx context.Context, chunk []string, emit toyreduce.Emitter) error {
	for _, line := range chunk {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		words := strings.Fields(line)
		for i := range words {
			if words[i] == "did" && i+1 < len(words) {
				emit(toyreduce.KeyValue{Key: words[i+1], Value: "1"})
				break
			}
		}
	}

	return nil
}

// Reduce aggregates the counts for each action
func (w ActionCountWorker) Reduce(ctx context.Context, key string, values []string, emit toyreduce.Emitter) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	emit(toyreduce.KeyValue{Key: key, Value: strconv.Itoa(len(values))})
	return nil
}

func (w ActionCountWorker) Description() string {
	return "Counts how many times each user action (after 'did') occurs, ignoring users"
}
