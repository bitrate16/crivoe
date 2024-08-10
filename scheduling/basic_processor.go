package scheduling

type BasicProcessor interface {
	Start() error
	Stop() error
	Add(task *BasicTask, callback *BasicCallback)
	GetWorker(workerTYpe string) (BasicWorker, error)
	RegisterWorker(workerTYpe string) error
}
