package pupupu

// Task status returned to user
type TaskStatus struct {
	// Assigned Task ID
	Id string `json:"id"`

	// Task status
	Status string `json:"status"`
}
