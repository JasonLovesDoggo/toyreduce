package toyreduce

import "errors"

// Sentinel errors for common error conditions
var (
	// Executor-related errors
	ErrInvalidExecutor  = errors.New("invalid executor specified")
	ErrExecutorNotFound = errors.New("executor not found")
	ErrUnknownExecutor  = errors.New("unknown executor")

	// Job-related errors
	ErrJobNotFound         = errors.New("job not found")
	ErrJobNotCompleted     = errors.New("job not completed")
	ErrJobAlreadyCancelled = errors.New("job already cancelled")

	// Version/compatibility errors
	ErrIncompatibleVersion = errors.New("incompatible version")

	// Storage/bucket errors
	ErrBucketNotFound            = errors.New("bucket not found")
	ErrDestinationBucketNotFound = errors.New("destination bucket not found")
	ErrStoreNotConfigured        = errors.New("store URL not configured")

	// HTTP/Network errors
	ErrStoreUnreachable        = errors.New("store unreachable")
	ErrRegistrationFailed      = errors.New("registration failed")
	ErrGetTaskFailed           = errors.New("get task failed")
	ErrCompleteTaskFailed      = errors.New("complete task failed")
	ErrHeartbeatFailed         = errors.New("heartbeat failed")
	ErrStoreMapOutputFailed    = errors.New("store map output failed")
	ErrGetReduceInputFailed    = errors.New("get reduce input failed")
	ErrStoreReduceOutputFailed = errors.New("store reduce output failed")
	ErrWorkerUnreachable       = errors.New("worker unreachable")
	ErrStoreReturnedError      = errors.New("store returned error")
	ErrStoreStatusError        = errors.New("store status error")
)
