package worker

import "crivoe/pupupu"

var registry map[string]pupupu.Worker

func RegisterWorker(workerType string, worker pupupu.Worker) {
	registry[workerType] = worker
}

func GetRegistry() map[string]pupupu.Worker {
	return registry
}

func init() {
	registry = make(map[string]pupupu.Worker)

	RegisterWorker("debug", &DebugWorker{})
}
