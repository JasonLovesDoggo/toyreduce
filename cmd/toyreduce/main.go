package main

import (
	"flag"
	"log"
	"path/filepath"
)

var (
	path = flag.String("path", "", "Path to the input csv file")
)

func main() {
	flag.Parse()

	if *path == "" {
		log.Fatal("path is required")
	}

	absPath, err := filepath.Abs(*path)
	if err != nil {
		panic(err)
	}
	println("Input file path:", absPath)
	println("Hello, ToyReduce!")
	println("This is a placeholder for the ToyReduce command-line tool.")
}
