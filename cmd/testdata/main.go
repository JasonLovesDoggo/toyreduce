package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"pkg.jsn.cam/toyreduce/cmd/testdata/generator"
)

var (
	GeneratorType = flag.String("generator", "actioncount", "Generator type (actioncount, wordcount, maxvalue, average, urldedup)")
	UserCount     = flag.Int("user_count", 100, "Number of unique users (for actioncount/wordcount)")
	TotalCount    = flag.Int64("total_count", 0, "Total number of lines to generate (0 = use generator default)")
	OutputPath    = flag.String("output", "var/testdata.log", "Output log file path")
	BufferSizeKB  = flag.Int("buffer_kb", 256, "Buffer size in KB for writer")
	Workers       = flag.Int("workers", runtime.NumCPU(), "Number of parallel workers")
	List          = flag.Bool("list", false, "List available generators and exit")
)

func main() {
	flag.Parse()

	// List generators if requested
	if *List {
		fmt.Println("Available generators:")
		for _, name := range generator.List() {
			gen, _ := generator.Get(name)
			fmt.Printf("  %-15s - %s (default: %s lines)\n", name, gen.Description(), humanize.Comma(gen.DefaultCount()))
		}
		return
	}

	// Apply user count if specified
	if *UserCount != 100 {
		generator.SetUserCount(*GeneratorType, *UserCount)
	}

	// Get generator
	gen, err := generator.Get(*GeneratorType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Available generators: %s\n", strings.Join(generator.List(), ", "))
		os.Exit(1)
	}

	// Determine count
	count := *TotalCount
	if count == 0 {
		count = gen.DefaultCount()
	}

	// Open file for writing
	if err := os.MkdirAll(filepath.Dir(*OutputPath), 0755); err != nil {
		panic(err)
	}
	file, err := os.Create(*OutputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Use bufio.Writer for efficient buffered I/O
	bufWriter := bufio.NewWriterSize(file, *BufferSizeKB*1024)
	defer bufWriter.Flush()

	startTime := time.Now()

	// Create progress bar with colors and theme
	bar := progressbar.NewOptions64(
		count,
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]%s[reset]", *GeneratorType)),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("lines/s"),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Streaming architecture: workers generate and write chunks as they complete
	// This avoids holding all 800M lines in memory at once
	chunkSize := count / int64(*Workers)
	if chunkSize == 0 {
		chunkSize = count
	}

	var writeMutex sync.Mutex
	var wg sync.WaitGroup

	// Worker goroutines generate and stream data in parallel
	for i := 0; i < *Workers; i++ {
		wg.Add(1)
		workerID := i
		start := int64(workerID) * chunkSize
		end := start + chunkSize
		if workerID == *Workers-1 {
			end = count // Last worker handles remainder
		}

		go func() {
			defer wg.Done()

			// Each worker gets its own generator instance and rand source
			gen, _ := generator.Get(*GeneratorType)
			if *UserCount != 100 {
				generator.SetUserCount(*GeneratorType, *UserCount)
				gen, _ = generator.Get(*GeneratorType)
			}

			// Create per-worker random source (eliminates lock contention)
			workerRand := rand.New(rand.NewPCG(uint64(workerID), uint64(workerID)*0xdeadbeef))
			gen.Init(workerRand)

			// Generate and flush in smaller batches to avoid memory bloat
			const batchSize = 100000 // 100K lines per batch
			buf := bytes.NewBuffer(make([]byte, 0, 1024*1024))

			linesInBatch := int64(0)
			for j := start; j < end; j++ {
				if err := gen.WriteLine(buf); err != nil {
					panic(err)
				}
				linesInBatch++

				// Flush when batch is full
				if linesInBatch >= batchSize || j == end-1 {
					writeMutex.Lock()
					if _, err := bufWriter.Write(buf.Bytes()); err != nil {
						writeMutex.Unlock()
						panic(err)
					}
					writeMutex.Unlock()

					bar.Add64(linesInBatch)
					buf.Reset()
					linesInBatch = 0
				}
			}
		}()
	}

	wg.Wait()
	bar.Finish()

	elapsed := time.Since(startTime)
	linesPerSec := float64(count) / elapsed.Seconds()

	fmt.Printf("\n✓ Generated %s lines in %v (%s lines/sec) → %s\n",
		humanize.Comma(count),
		elapsed.Round(time.Millisecond),
		humanize.Comma(int64(linesPerSec)),
		*OutputPath)
}
