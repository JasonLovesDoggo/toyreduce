package protocol

import (
	"fmt"

	"golang.org/x/mod/semver"
)

const ToyReduceVersion = "v0.2.0"

// IsCompatibleVersion checks if a worker version is compatible with the master version.
// Compatibility rules:
// - Major version must match exactly.
// - Minor and patch versions can differ.
func IsCompatibleVersion(workerVersion, masterVersion string) (bool, error) {
	if !semver.IsValid(workerVersion) {
		return false, fmt.Errorf("invalid worker version: %s", workerVersion)
	}
	if !semver.IsValid(masterVersion) {
		return false, fmt.Errorf("invalid master version: %s", masterVersion)
	}

	workerMajor := semver.Major(workerVersion)
	masterMajor := semver.Major(masterVersion)

	return workerMajor == masterMajor, nil
}

// GetCompatibilityError returns a user-friendly message for incompatible versions.
func GetCompatibilityError(workerVersion, masterVersion string) string {
	return fmt.Sprintf(
		"Worker version %s is incompatible with master version %s. Required version: %s.x.x",
		workerVersion, masterVersion, semver.Major(masterVersion),
	)
}
