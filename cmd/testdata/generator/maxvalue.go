package generator

import (
	"fmt"
	"io"
	"math/rand/v2"
)

// MaxValueGenerator generates key:value pairs for finding maximum values
type MaxValueGenerator struct {
	KeyCount int
	rand     *rand.Rand
}

var metricKeys = []string{
	"temperature",
	"humidity",
	"pressure",
	"cpu_usage",
	"memory_usage",
	"disk_io",
	"network_latency",
	"response_time",
	"error_rate",
	"request_count",
}

func (g *MaxValueGenerator) Init(r *rand.Rand) {
	g.rand = r
}

func (g *MaxValueGenerator) WriteLine(w io.Writer) error {
	key := metricKeys[g.rand.IntN(len(metricKeys))]
	// Generate values between 0 and 100 with 2 decimal places
	value := g.rand.Float64() * 100
	_, err := fmt.Fprintf(w, "%s:%.2f\n", key, value)
	return err
}

func (g *MaxValueGenerator) Description() string {
	return "Metric data: key:value (for max/average operations)"
}

func (g *MaxValueGenerator) DefaultCount() int64 {
	return 1e5 // 100,000 lines
}
