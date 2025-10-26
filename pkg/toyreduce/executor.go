package toyreduce

import (
	"bufio"
	"os"
)

/*
JOB TO DO.

take file.

split into chunks.

send each chunk to <interface> Worker for map.

then shuffle (organize by key).

send each key group to <interface> Worker for reduce.

how does return work?
*/

/*
1. Chunk file
2. Map: each chunk → one worker → emits (key, value) pairs.
3. Shuffle: merge all outputs, group by key.
4. Reduce: send each key → [values] to a reducer worker → emits results.
5. Collect: combine all reducer outputs → final result.

*/
// Chunk splits a file into line-preserving chunks roughly chunkSizeMB each.
// It sends each chunk to the out channel and closes it when done.
func Chunk(filePath string, chunkSizeMB int, out chan<- []string) error {
	f, err := os.Open(filePath)
	if err != nil {
		close(out)
		return err
	}

	defer f.Close()
	defer close(out)

	targetBytes := int64(chunkSizeMB) * 1024 * 1024
	sc := bufio.NewScanner(f)

	var (
		chunk      []string
		chunkBytes int64
	)

	for sc.Scan() {
		line := sc.Text()
		lineBytes := int64(len(line) + 1) // include newline

		switch {
		case lineBytes > targetBytes && len(chunk) == 0:
			// single huge line, must emit as-is
			out <- []string{line}
		case chunkBytes+lineBytes > targetBytes && len(chunk) > 0:
			out <- chunk

			chunk = []string{line}
			chunkBytes = lineBytes
		default:
			chunk = append(chunk, line)
			chunkBytes += lineBytes
		}
	}

	if err := sc.Err(); err != nil {
		return err
	}

	if len(chunk) > 0 {
		out <- chunk
	}

	return nil
}

func MapPhase(chunks <-chan []string, worker Worker) ([]KeyValue, error) {
	var all []KeyValue

	for chunk := range chunks {
		var chunkOutput []KeyValue

		err := worker.Map(chunk, func(kv KeyValue) {
			chunkOutput = append(chunkOutput, kv)
		})
		if err != nil {
			return nil, err
		}

		// Apply combine phase to this chunk's output to reduce intermediate data
		combined, err := CombinePhase(chunkOutput, worker)
		if err != nil {
			return nil, err
		}

		all = append(all, combined...)
	}

	return all, nil
}

func Shuffle(pairs []KeyValue) map[string][]string {
	grouped := make(map[string][]string)
	for _, kv := range pairs {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}

	return grouped
}

// CombinePhase applies local aggregation to reduce intermediate data.
// Execution order:
// 1. Worker implements DisableCombiner() returning true → skip combining
// 2. Otherwise, if Worker implements CombinableWorker with Combine() → use custom logic
// 3. Otherwise, use the worker's Reduce() function as combiner
func CombinePhase(pairs []KeyValue, worker Worker) ([]KeyValue, error) {
	// Check if worker opts-out of combining
	if disabler, ok := worker.(DisableCombinerCheck); ok {
		if disabler.DisableCombiner() {
			return pairs, nil // Skip combining
		}
	}

	// Shuffle pairs locally by key
	grouped := Shuffle(pairs)

	// Combine using appropriate function
	var combined []KeyValue

	emitter := func(kv KeyValue) {
		combined = append(combined, kv)
	}

	for key, values := range grouped {
		// Check if worker has custom Combine() method
		if combinable, ok := worker.(CombinableWorker); ok {
			if err := combinable.Combine(key, values, emitter); err != nil {
				return nil, err
			}
		} else {
			// Default: use Reduce() for combining
			if err := worker.Reduce(key, values, emitter); err != nil {
				return nil, err
			}
		}
	}

	return combined, nil
}

func ReducePhase(groups map[string][]string, worker Worker) []KeyValue {
	var results []KeyValue

	for key, values := range groups {
		worker.Reduce(key, values, func(kv KeyValue) {
			results = append(results, kv)
		})
	}

	return results
}
