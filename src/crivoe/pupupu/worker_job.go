package pupupu

// Container
type WorkerJob struct {
	// Assigned ID
	Id string

	// Real Job
	Job *Job

	// Callback for result
	Callback Callback
}
