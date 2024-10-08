package pupupu

// Job metadata returned to user
type JobMetadata struct {
	// Assigned ID of Job
	Id string `json:"id"`

	// Job Status
	Status string `json:"status"`

	// Job metadata
	Metadata interface{} `json:"metadata"`
}
