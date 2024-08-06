package scheduling

import "fmt"

// Stateless Interface for task worker, implementing specific task type logic
type Worker interface {
	Launch(batch *Batch, task *Task, callback TaskCallback)
}

// Anonymous function interface implementation
type WorkerHandler func(batch *Batch, task *Task, callback TaskCallback)

func (wh WorkerHandler) Launch(batch *Batch, task *Task, callback TaskCallback) {
	wh(batch, task, callback)
}

// Local storage with all supported worker types
var workerRegistry = make(map[string]Worker)

// Add worker type to local registry
func RegisterWorkerType(workerType string, worker Worker) error {
	if _, has := workerRegistry[workerType]; has {
		return fmt.Errorf("worker for %s already exists", workerType)
	}

	workerRegistry[workerType] = worker

	return nil
}

// Get worker from local registry
func GetWorkerForType(workerType string) (Worker, error) {
	if worker, has := workerRegistry[workerType]; has {
		return worker, nil
	}

	return nil, fmt.Errorf("worker for %s does not exist", workerType)
}

// TODO: Add WorkerStatus to task & batch. required for tracking task retry/delay/execution state

// import (
// 	"errors"
// 	"fmt"
// 	"sync"
// )

// type Worker struct {
// 	lock      sync.Mutex
// 	quit      chan struct{}
// 	isRunning bool
// 	queue     *Queue
// }

// func NewWorker() *Worker {
// 	return &Worker{
// 		isRunning: false,
// 	}
// }

// func (w *Worker) workerLoop() {
// 	for {
// 		select {
// 		case <-w.quit:
// 			return
// 		case task := <-w.queue.queue:
// 			fmt.Printf("Task: %v\n", task)
// 			panic("not implemented")
// 		}
// 	}
// }

// func (w *Worker) Start() error {
// 	if w == nil {
// 		return errors.New("worker is nil")
// 	}

// 	w.lock.Lock()
// 	defer w.lock.Unlock()

// 	if w.isRunning {
// 		return errors.New("worker already running")
// 	}

// 	w.quit = make(chan struct{})
// 	w.isRunning = true
// 	go w.workerLoop()

// 	return nil
// }

// func (w *Worker) Stop() error {
// 	if w == nil {
// 		return errors.New("worker is nil")
// 	}

// 	w.lock.Lock()
// 	defer w.lock.Unlock()

// 	if !w.isRunning {
// 		return errors.New("worker not running")
// 	}

// 	w.quit <- struct{}{}
// 	close(w.quit)
// 	w.isRunning = false

// 	return nil
// }

// func (w *Worker) setQueue(queue *Queue) error {
// 	if w == nil {
// 		return errors.New("worker is nil")
// 	}

// 	w.lock.Lock()
// 	defer w.lock.Unlock()

// 	if w.isRunning {
// 		return errors.New("can not change queue of running worker")
// 	}

// 	w.queue = queue

// 	return nil
// }
