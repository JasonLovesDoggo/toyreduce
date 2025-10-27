package toyreduce

type Config struct {
	Worker        Worker
	InputFilePath string
	ChunkSize     int
	Workers       int
}
