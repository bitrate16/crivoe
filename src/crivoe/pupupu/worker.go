package pupupu

// TODO: Excessive parameter `callback`
// Stateless Interface for task worker, implementing specific task type logic
type Worker interface {
	Launch(task *WorkerTask, master Master, callback Callback)
}

// Anonymous function interface implementation
type WorkerHandler func(task *WorkerTask, master Master, callback Callback)

func (wh WorkerHandler) Launch(task *WorkerTask, master Master, callback Callback) {
	wh(task, master, callback)
}
