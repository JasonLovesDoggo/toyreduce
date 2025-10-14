package toyreduce

type Config struct {
	InputFilePath string
	ChunkSize     int
	Workers       int
	Worker        Worker
}
