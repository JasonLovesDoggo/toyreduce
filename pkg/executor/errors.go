package executor

import "errors"

var FileNotFoundError = errors.New("file not found")
var InvalidChunkSizeError = errors.New("invalid chunk size")
var MapError = errors.New("error during map phase")
var ReduceError = errors.New("error during reduce phase")
var EmitError = errors.New("error during emit phase")
var ShuffleError = errors.New("error during shuffle phase")
var ChunkingError = errors.New("error during chunking phase")
