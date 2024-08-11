package pupupu

// Task specification returned to user
type TaskSpec struct {
	// From `Task.Type`
	Type string `json:"type"`

	// Assigned Task ID
	Id string `json:"id"`

	// Jobs of the task
	JobSpecs []*JobSpec `json:"job_specs"`
}
