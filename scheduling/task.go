package scheduling

type Task struct {
	// Type of the task, defines which worker to use
	Type string `json:"type"`

	// Task options based on worker requirements
	Options interface{} `json:"options"`
}

// Pre defined status: undefined
const TaskStatusUndefined = "UNDEFINED"

// Pre defined status: task completed
const TaskStatusComplete = "COMPLETE"

// Pre defined status: task failed
const TaskStatusFail = "FAIL"

// Pre defined status: task finished with error. `Result` contains error
const TaskStatusError = "ERROR"

// Contains status returned by task
type TaskStatus struct {
	// Definitive status for task
	Status string `json:"status"`

	// Result of task, optional
	Result interface{} `json:"result"`
}

// Callback must be called when task finishes
type TaskCallback interface {
	Done(status *TaskStatus)
}

// Anonymous function interface implementation
type TaskCallbackHandler func(status *TaskStatus)

func (tch TaskCallbackHandler) Done(status *TaskStatus) {
	tch(status)
}
