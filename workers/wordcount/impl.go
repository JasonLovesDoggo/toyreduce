package wordcount

import (
	"strconv"
	"strings"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

type WordCountWorker struct{}

func (w WordCountWorker) Map(lines []string) []toyreduce.KeyValue {
	var out []toyreduce.KeyValue
	for _, line := range lines {
		for _, word := range strings.Fields(line) {
			out = append(out, toyreduce.KeyValue{Key: word, Value: "1"})
		}
	}
	return out
}

func (w WordCountWorker) Reduce(key string, values []string) toyreduce.KeyValue {
	return toyreduce.KeyValue{Key: key, Value: strconv.Itoa(len(values))}
}
