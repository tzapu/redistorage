package redistorage

import "time"

// QueueConfig configures the queue settings
type QueueConfig struct {
	BlockingDuration  time.Duration
	ProcessingTimeout time.Duration
}
