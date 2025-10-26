package generator

import (
	"io"
	"math/rand/v2"
)

// Generator produces test data for different executor types
type Generator interface {
	// Init initializes the generator with a per-instance random source
	// This eliminates lock contention on the global rand source
	Init(r *rand.Rand)

	// WriteLine writes a single line of test data to the writer
	WriteLine(w io.Writer) error

	// Description returns a human-readable description of the data format
	Description() string

	// DefaultCount returns the suggested default number of lines to generate
	DefaultCount() int64
}
