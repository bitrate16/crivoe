package pupupu

// Job status returned to user
type JobStatus struct {
	// Assigned ID of Job
	Id string `json:"id"`

	// Job status
	Status string `json:"status"`
}
