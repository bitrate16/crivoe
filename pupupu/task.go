package pupupu

// User-defined task specification
type Task struct {
	// Worker type for this task
	Type string `json:"type"`

	// Max retries of the task
	MaxRetries int `json:"max_retries"`

	// Options fot the task
	Options interface{} `json:"options"`

	// Jobs of the task
	Jobs []*Job `json:"jobs"`
}
