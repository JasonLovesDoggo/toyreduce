package generator

import (
	"io"
	"math/rand/v2"
)

// URLDedupGenerator generates URLs with various domains for deduplication testing
type URLDedupGenerator struct {
	rand *rand.Rand
}

var domains = []string{
	"google.com",
	"facebook.com",
	"twitter.com",
	"github.com",
	"stackoverflow.com",
	"reddit.com",
	"youtube.com",
	"linkedin.com",
	"amazon.com",
	"wikipedia.org",
}

var paths = []string{
	"",
	"/home",
	"/about",
	"/contact",
	"/products",
	"/api/v1",
	"/api/v2",
	"/docs",
	"/blog",
	"/search",
	"/user/profile",
	"/settings",
	"/help",
}

var params = []string{
	"",
	"?page=1",
	"?id=123",
	"?ref=homepage",
	"?utm_source=test",
	"?sort=desc",
}

var httpsPrefix = []byte("https://")
var newlineBytes = []byte("\n")

func (g *URLDedupGenerator) Init(r *rand.Rand) {
	g.rand = r
}

func (g *URLDedupGenerator) WriteLine(w io.Writer) error {
	domain := domains[g.rand.IntN(len(domains))]
	path := paths[g.rand.IntN(len(paths))]
	param := params[g.rand.IntN(len(params))]

	if _, err := w.Write(httpsPrefix); err != nil {
		return err
	}
	if _, err := io.WriteString(w, domain); err != nil {
		return err
	}
	if _, err := io.WriteString(w, path); err != nil {
		return err
	}
	if _, err := io.WriteString(w, param); err != nil {
		return err
	}
	_, err := w.Write(newlineBytes)
	return err
}

func (g *URLDedupGenerator) Description() string {
	return "URLs for deduplication: https://domain.com/path?params"
}

func (g *URLDedupGenerator) DefaultCount() int64 {
	return 5e4 // 50,000 lines with lots of duplicates
}
