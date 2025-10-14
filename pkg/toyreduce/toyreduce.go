package toyreduce

import (
	"fmt"
	"log"
)

func Run(cfg Config) {
	chunks := make(chan []string, cfg.Workers)
	go func() {
		if err := Chunk(cfg.InputFilePath, cfg.ChunkSize, chunks); err != nil {
			log.Fatal(err)
		}
	}()

	mapped, err := MapPhase(chunks, cfg.Worker)
	if err != nil {
		log.Fatal(err)
	}

	grouped := Shuffle(mapped)
	reduced := ReducePhase(grouped, cfg.Worker)

	for _, kv := range reduced {
		fmt.Printf("%s: %s\n", kv.Key, kv.Value)
	}
}
