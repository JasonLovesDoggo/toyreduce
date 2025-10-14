package toyreduce

import (
	"log"

	"pkg.jsn.cam/toyreduce/pkg/executor"
)

func Run(config Config) {
	const numChunks = 10 // todo: calculate based off of size/chunk size
	var outputChunks = make(chan []string, numChunks)
	err := executor.Chunk(config.InputFilePath, config.ChunkSize, outputChunks)
	if err != nil {
		log.Fatal("Error during chunking:", err)
	}
	//executor.Map(outputChunks, config.Worker.Map)
	println("ToyReduce Run function executed.")
}
