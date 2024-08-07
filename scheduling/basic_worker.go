package scheduling

// Stateless Interface for task worker, implementing specific task type logic
type BasicWorker interface {
	Launch(task *BasicTask, callback BasicCallback)
}

// Anonymous function interface implementation
type BasicWorkerHandler func(task *BasicTask, callback BasicCallback)

func (bwh BasicWorkerHandler) Launch(task *BasicTask, callback BasicCallback) {
	bwh(task, callback)
}
