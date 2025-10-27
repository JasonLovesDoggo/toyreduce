package storage

import "testing"

func TestMemoryBackend(t *testing.T) {
	t.Parallel()
	backendTestSuite(t, func() (Backend, func(), error) {
		backend := NewMemoryBackend()
		return backend, func() {}, nil
	})
}
