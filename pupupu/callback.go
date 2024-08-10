package pupupu

// Task processing result
type TaskResult struct {
	// Task status
	Status string `json:"status"`

	// Optional result
	Result interface{} `json:"result"`

	// Reference to original Task
	Task *WorkerTask `json:"task"`
}

// Job processing result
type JobResult struct {
	// Job status
	Status string `json:"status"`

	// Optional result
	Result interface{} `json:"result"`

	// Reference to original Job
	Job *WorkerJob `json:"job"`
}

// Callback for worker result
type Callback interface {
	TaskCallback(taskResult *TaskResult)
	JobCallback(jobResult *JobResult)
}

// Container
type CallbackWrap struct {
	taskCallback func(taskResult *TaskResult)
	jobCallback  func(jobResult *JobResult)
}

func (c *CallbackWrap) TaskCallback(taskResult *TaskResult) {
	c.taskCallback(taskResult)
}

func (c *CallbackWrap) JobCallback(jobResult *JobResult) {
	c.jobCallback(jobResult)
}

func NewCallbackWrap(
	taskCallback func(taskResult *TaskResult),
	jobCallback func(jobResult *JobResult),
) *CallbackWrap {
	return &CallbackWrap{
		taskCallback: taskCallback,
		jobCallback:  jobCallback,
	}
}
