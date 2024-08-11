package pupupu

// TODO: Excessive parameter `callback`
// Stateless Interface for task worker, implementing specific task type logic
type Worker interface {
	Launch(task *WorkerTask, master Master, callback Callback)
}
