package scheduling

type Batch struct {
	// Array of tasks to process
	Tasks []*Task `json:"tasks"`

	// Max retries per task
	MaxRetries int `json:"max_retries"`

	// Delay between tasks in single parallel row
	TaskDelay int `json:"task_delay"`

	// Options for entire batch
	Options interface{} `json:"options"`
}

// Pre defined status: undefined
const BatchStatusUndefined = "UNDEFINED"

// Pre defined status: batch completed
const BatchStatusComplete = "COMPLETE"

// Pre defined status: batch failed
const BatchStatusFail = "FAIL"

// Pre defined status: batch finished with error. `Result` contains error
const BatchStatusError = "ERROR"

// Contains status returned by batch
type BatchStatus struct {
	// Definitive status for task
	Status string `json:"status"`

	// Result of task, optional
	Result interface{} `json:"result"`
}

// Callback must be called when batch finishes
type BatchCallback interface {
	// Called after finish of each task in batch. Can modify state of `batchStatus`
	Partial(batchStatus *BatchStatus, taskStatus *TaskStatus)

	// Entire batch finished. Returns `batchStatus` right after last call of `Partial`
	Done(status *BatchStatus)
}
