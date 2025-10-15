package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBboltBackend(t *testing.T) {
	backendTestSuite(t, func() (Backend, func(), error) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		backend, err := NewBboltBackend(dbPath)
		if err != nil {
			return nil, nil, err
		}

		cleanup := func() {
			backend.Close()
			os.Remove(dbPath)
		}

		return backend, cleanup, nil
	})
}
