package pupupu

// Container
type WorkerTask struct {
	// Assigned ID
	Id string

	// Real Task
	Task *Task

	// Container jobs
	Jobs []*WorkerJob

	// Callback for result
	Callback Callback
}
