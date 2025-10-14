package workers

import (
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/workers/actioncount"
)

import "pkg.jsn.cam/toyreduce/workers/wordcount"

var Executors = map[string]toyreduce.Worker{
	"wordcount":   wordcount.WordCountWorker{},
	"actioncount": actioncount.ActionCountWorker{},
}

func IsValidExecutor(name string) bool {
	_, exists := Executors[name]
	return exists
}
func GetWorker(name string) toyreduce.Worker {
	return Executors[name]
}

func ListExecutors() []string {
	var names []string
	for name := range Executors {
		names = append(names, name)
	}
	return names
}

func GetDescription(name string) (string, error) {
	if worker, exists := Executors[name]; exists {
		return worker.Description(), nil
	}
	return "", toyreduce.InvalidExecutorError
}
