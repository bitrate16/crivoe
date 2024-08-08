package scheduling

import "time"

// User-defined task specification
type BasicTask struct {
	// Worker type for this task
	Type string `json:"type"`

	// Max retries of the task
	MaxRetries int `json:"max_retries"`

	// Delay between retries in milliseconds
	RetryDelay time.Duration `json:"retry_delay"`

	// Options fot the task
	Options interface{} `json:"options"`
}
