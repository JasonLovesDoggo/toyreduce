package average

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// AverageWorker calculates the average numeric value per key.
// Input format: "key:value" per line (e.g., "temperature:72.5")
// Demonstrates custom Combine logic to track sum and count.
type AverageWorker struct{}

// Map extracts key-value pairs and emits (key, value)
func (w AverageWorker) Map(ctx context.Context, chunk []string, emit toyreduce.Emitter) error {
	for _, line := range chunk {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			emit(toyreduce.KeyValue{Key: parts[0], Value: parts[1]})
		}
	}

	return nil
}

// Combine aggregates values locally by computing sum and count.
// Emits (key, "sum:count") to allow downstream reducers to compute final average.
func (w AverageWorker) Combine(ctx context.Context, key string, values []string, emit toyreduce.Emitter) error {
	var (
		sum   float64
		count int
	)

	for _, v := range values {
		// Check if value is already in "sum:count" format (from previous combine)
		if strings.Contains(v, ":") {
			parts := strings.SplitN(v, ":", 2)
			if len(parts) == 2 {
				s, _ := strconv.ParseFloat(parts[0], 64)
				c, _ := strconv.Atoi(parts[1])
				sum += s
				count += c
			}
		} else {
			// Raw value from Map phase
			val, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err == nil {
				sum += val
				count++
			}
		}
	}

	if count > 0 {
		// Emit as "sum:count" format for further aggregation
		emit(toyreduce.KeyValue{Key: key, Value: fmt.Sprintf("%f:%d", sum, count)})
	}

	return nil
}

// Reduce computes the final average from aggregated sum:count pairs
func (w AverageWorker) Reduce(ctx context.Context, key string, values []string, emit toyreduce.Emitter) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	var (
		totalSum   float64
		totalCount int
	)

	for _, v := range values {
		// Values should be in "sum:count" format from Combine
		parts := strings.SplitN(v, ":", 2)
		if len(parts) == 2 {
			s, _ := strconv.ParseFloat(parts[0], 64)
			c, _ := strconv.Atoi(parts[1])
			totalSum += s
			totalCount += c
		} else {
			// Handle raw values (if no combine was run)
			val, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err == nil {
				totalSum += val
				totalCount++
			}
		}
	}

	if totalCount > 0 {
		avg := totalSum / float64(totalCount)
		emit(toyreduce.KeyValue{Key: key, Value: strconv.FormatFloat(avg, 'f', 2, 64)})
	}

	return nil
}

func (w AverageWorker) Description() string {
	return "Calculates average numeric value per key (format: key:value)"
}
