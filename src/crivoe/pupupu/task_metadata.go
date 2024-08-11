package pupupu

// Task metadata returned to user
type TaskMetadata struct {
	// Assigned Task ID
	Id string `json:"id"`

	// Task Statis
	Status string `json:"status"`

	// Task metadata
	Metadata interface{} `json:"metadata"`
}
