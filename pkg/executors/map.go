package workers

import (
	"pkg.jsn.cam/toyreduce/pkg/executors/actioncount"
	"pkg.jsn.cam/toyreduce/pkg/executors/average"
	"pkg.jsn.cam/toyreduce/pkg/executors/maxvalue"
	"pkg.jsn.cam/toyreduce/pkg/executors/urldedup"
	"pkg.jsn.cam/toyreduce/pkg/executors/wordcount"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

var Executors = map[string]toyreduce.Worker{
	"wordcount":   wordcount.WordCountWorker{},
	"actioncount": actioncount.ActionCountWorker{},
	"maxvalue":    maxvalue.MaxValueWorker{},
	"urldedup":    urldedup.URLDedupWorker{},
	"average":     average.AverageWorker{},
}

func IsValidExecutor(name string) bool {
	_, exists := Executors[name]
	return exists
}
func GetExecutor(name string) toyreduce.Worker {
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
