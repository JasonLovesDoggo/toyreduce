package main

import (
	"flag"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
)

/*generates a ton of test data in the form of {user_id} did {action}*/

var (
	UserCount  = flag.Int("user_count", 100, "Number of unique users")
	TotalCount = flag.Int64("total_count", 1e4, "Total number of actions to generate")
	OutputPath = flag.String("output", "var/testdata.log", "Output log file path")
)

var Actions = []string{
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

const userPrefix = "user_"

func GenerateUserID() string {
	return userPrefix + strconv.Itoa(rand.IntN(*UserCount))
}

func generateRow() string {
	return GenerateUserID() + " did " + Actions[rand.IntN(len(Actions))] + "\n"
}

func main() {
	flag.Parse()

	// open file for writing
	if err := os.MkdirAll(filepath.Dir(*OutputPath), 0755); err != nil {
		panic(err)
	}
	file, err := os.Create(*OutputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// generate data
	for i := int64(0); i < *TotalCount; i++ {
		_, err := file.WriteString(generateRow())
		if err != nil {
			panic(err)
		}
	}
}
