package generator

import "fmt"

// Registry maps generator names to generator factory functions
// We use factory functions to allow parameterization (e.g., UserCount)
var Registry = map[string]func() Generator{
	"actioncount": func() Generator { return &ActionCountGenerator{UserCount: 100} },
	"wordcount":   func() Generator { return &ActionCountGenerator{UserCount: 100} }, // Same format as actioncount
	"maxvalue":    func() Generator { return &MaxValueGenerator{KeyCount: 10} },
	"average":     func() Generator { return &MaxValueGenerator{KeyCount: 10} }, // Same format as maxvalue
	"urldedup":    func() Generator { return &URLDedupGenerator{} },
}

// Get returns a generator by name
func Get(name string) (Generator, error) {
	factory, exists := Registry[name]
	if !exists {
		return nil, fmt.Errorf("unknown generator: %s", name)
	}
	return factory(), nil
}

// List returns all available generator names
func List() []string {
	var names []string
	for name := range Registry {
		names = append(names, name)
	}
	return names
}

// SetUserCount updates the UserCount for ActionCountGenerator
func SetUserCount(name string, count int) {
	if name == "actioncount" || name == "wordcount" {
		Registry[name] = func() Generator { return &ActionCountGenerator{UserCount: count} }
	}
}

// SetKeyCount updates the KeyCount for MaxValueGenerator
func SetKeyCount(name string, count int) {
	if name == "maxvalue" || name == "average" {
		Registry[name] = func() Generator { return &MaxValueGenerator{KeyCount: count} }
	}
}
