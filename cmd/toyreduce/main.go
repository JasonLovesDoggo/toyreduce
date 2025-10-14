package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/workers"
)

var (
	executor    = flag.String("executor", "", "Executor to use (run 'toyreduce list-executors' to see available executors)")
	path        = flag.String("path", "", "Path to the input csv file")
	chunkSize   = flag.Int("chunk-size", 1000, "Number of lines per chunk")
	workerCount = flag.Int("workers", 4, "Number of workers")
)

func main() {
	flag.Parse()
	handleSubCommands()

	validateFlags()

	absPath, err := filepath.Abs(*path)
	if err != nil {
		panic(err)
	}

	toyConfig := toyreduce.Config{
		InputFilePath: absPath,
		ChunkSize:     *chunkSize,
		Workers:       *workerCount,
		Worker:        workers.GetWorker(*executor),
	}
	toyreduce.Run(toyConfig)
}

func handleSubCommands() {
	if len(flag.Args()) >= 1 {
		switch flag.Args()[0] {
		case "list-executors":
			fmt.Println("Available executors:")
			for _, name := range workers.ListExecutors() {
				desc, err := workers.GetDescription(name)
				if err != nil {
					log.Fatal(err) // should not happen
				}
				fmt.Println(" -", name, "("+desc+")")
			}
		}
		os.Exit(0)

	}

}

func validateFlags() {

	if *executor == "" {
		log.Fatal("executor is required (run 'toyreduce list-executors' to see available executors)")
	}

	if !workers.IsValidExecutor(*executor) {
		log.Fatalf("Invalid executor: %s. Run 'toyreduce list-executors' to see available executors.", *executor)
	}

	if *path == "" {
		log.Fatal("path is required")
	}

}
