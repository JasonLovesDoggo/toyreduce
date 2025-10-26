package urldedup

import (
	"net/url"
	"strings"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// URLDedupWorker deduplicates URLs per domain.
// Input: URLs, one per line
// Output: (domain, unique_url_count)
type URLDedupWorker struct{}

// Map extracts domain from each URL and emits (domain, url)
func (w URLDedupWorker) Map(chunk []string, emit toyreduce.Emitter) error {
	for _, line := range chunk {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse URL to extract domain
		u, err := url.Parse(line)
		if err != nil {
			continue // Skip invalid URLs
		}

		domain := u.Host
		if domain == "" {
			continue
		}

		emit(toyreduce.KeyValue{Key: domain, Value: line})
	}
	return nil
}

// Reduce deduplicates URLs and counts unique URLs per domain
func (w URLDedupWorker) Reduce(key string, values []string, emit toyreduce.Emitter) error {
	seen := make(map[string]struct{})
	for _, urlvalue := range values {
		seen[urlvalue] = struct{}{}
	}

	// Emit count of unique URLs for this domain
	emit(toyreduce.KeyValue{Key: key, Value: string(rune(len(seen)))})
	return nil
}

// DisableCombiner prevents combining because we need to see ALL URLs
// before we can properly deduplicate. Combining would give incorrect results
// since the same URL might appear in multiple chunks.
func (w URLDedupWorker) DisableCombiner() bool {
	return true
}

func (w URLDedupWorker) Description() string {
	return "Deduplicates URLs per domain and counts unique URLs (no combining)"
}
