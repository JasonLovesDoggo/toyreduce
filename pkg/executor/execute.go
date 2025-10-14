package executor

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

func Chunk(filePath string, chunkSize int, out chan<- []string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var chunk []string
	for scanner.Scan() {
		chunk = append(chunk, scanner.Text())
		if len(chunk) >= chunkSize {
			out <- chunk // push one chunk
			chunk = nil
		}
	}
	if len(chunk) > 0 {
		out <- chunk
	}
	close(out)
	return scanner.Err()
}

func Map(chunks <-chan []string, mapFunc func([]string) ([][2]string, error), out chan<- [][2]string) error {
	for chunk := range chunks {
		mappedData, err := mapFunc(chunk)
		if err != nil {
			return err
		}
		out <- mappedData
	}
	return nil
}
