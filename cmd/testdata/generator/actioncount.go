package generator

import (
	"io"
	"math/rand/v2"
	"strconv"
)

// ActionCountGenerator generates user action logs in format: "{user_id} did {action}"
type ActionCountGenerator struct {
	UserCount int
	rand      *rand.Rand
	linePool  [][]byte // Pre-generated complete lines
}

var actions = []string{
	"login",
	"logout",
	"viewed product",
	"added to cart",
	"removed from cart",
	"purchased",
	"reviewed product",
	"updated profile",
	"changed password",
	"subscribed to newsletter",
}

const linePoolSize = 10000 // Pre-generate this many unique lines

func (g *ActionCountGenerator) Init(r *rand.Rand) {
	g.rand = r

	// Pre-generate user IDs
	userIDs := make([]string, g.UserCount)
	for i := 0; i < g.UserCount; i++ {
		userIDs[i] = "user_" + strconv.Itoa(i)
	}

	// Pre-generate a pool of complete lines
	g.linePool = make([][]byte, linePoolSize)
	for i := 0; i < linePoolSize; i++ {
		userID := userIDs[r.IntN(g.UserCount)]
		action := actions[r.IntN(len(actions))]
		line := userID + " did " + action + "\n"
		g.linePool[i] = []byte(line)
	}
}

func (g *ActionCountGenerator) WriteLine(w io.Writer) error {
	// Pick a random pre-generated line and write it
	line := g.linePool[g.rand.IntN(linePoolSize)]
	_, err := w.Write(line)
	return err
}

func (g *ActionCountGenerator) Description() string {
	return "User action logs: {user_id} did {action}"
}

func (g *ActionCountGenerator) DefaultCount() int64 {
	return 1e4 // 10,000 lines
}
