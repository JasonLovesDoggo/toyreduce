package master

import (
	"log"

	"pkg.jsn.cam/toyreduce/pkg/storage"
)

// NewBboltStorage creates a new bbolt-backed storage
func NewBboltStorage(dbPath string) (Storage, error) {
	backend, err := storage.NewBboltBackend(dbPath)
	if err != nil {
		return nil, err
	}

	log.Printf("[STORAGE] Bbolt storage initialized at %s", dbPath)

	return NewMasterStorage(backend)
}
