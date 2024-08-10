package scheduling

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Defines single basicQueueTask in queue
type basicQueueTask struct {
	task     *BasicTask
	callback BasicCallback
	retry    int
}

// Basic queue object
type GoroutineQueue struct {
	lock    sync.Mutex
	quit    chan struct{}
	queue   chan *basicQueueTask
	running bool
	workers map[string]BasicWorker
}

func (q *GoroutineQueue) queueLoop() {
	for {
		select {
		case <-q.quit:
			return

		case task := <-q.queue:
			// Find worker for task
			worker, err := q.GetWorker(task.task.Type)

			// Fail on noexistent worker
			if err != nil {
				task.callback.Done(
					&BasicCallbackResult{
						Status: BasicCallbackStatusFail,
						Result: err,
					},
				)
				continue
			}

			// Fire task
			worker.Launch(
				task.task,
				BasicCallbackFunc(func(result *BasicCallbackResult) {

					// Return status if not error
					if result.Status != BasicCallbackStatusError {
						task.callback.Done(result)
						return
					}

					// Try reschedule in other case
					if task.retry < task.task.MaxRetries {
						task.retry += 1

						// Reschedule somewhere in the future to prevent blocking
						go func() {

							// Wait before reschedule
							time.Sleep(task.task.RetryDelay * time.Millisecond)

							// TODO: Check if this prevents writing to channel when queue is not running
							q.lock.Lock()
							defer q.lock.Unlock()

							if !q.running {
								// Notify cancelled
								result.Status = BasicCallbackStatusCancel
								task.callback.Done(result)
								return
							}

							// Reschedule
							q.queue <- task
						}()
					} else {
						// Notify of failure, rewrite status
						result.Status = BasicCallbackStatusFail
						task.callback.Done(result)
					}
				}),
			)
		}
	}
}

func NewBasicQueue() *GoroutineQueue {
	return &GoroutineQueue{
		workers: make(map[string]BasicWorker),
		running: false,
	}
}

// Register specific worker, not thread-safe
func (q *GoroutineQueue) RegisterWorker(workerType string, worker BasicWorker) error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.workers[workerType] = worker

	return nil
}

// Get specific worker, not thread-safe
func (q *GoroutineQueue) GetWorker(workerType string) (BasicWorker, error) {
	if q == nil {
		return nil, errors.New("queue is nil")
	}

	if worker, ok := q.workers[workerType]; ok {
		return worker, nil
	}

	return nil, fmt.Errorf("worker for %s does not exist", workerType)
}

func (q *GoroutineQueue) Start() error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	if q.running {
		return errors.New("queue already running")
	}

	q.quit = make(chan struct{})
	q.queue = make(chan *basicQueueTask)
	q.running = true

	go q.queueLoop()

	return nil
}

func (q *GoroutineQueue) Stop() error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	if !q.running {
		return errors.New("queue not running")
	}

	q.running = false
	q.quit <- struct{}{}
	close(q.quit)
	close(q.queue)

	return nil
}

func (q *GoroutineQueue) Add(task *BasicTask, callback BasicCallback) error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	if !q.running {
		return errors.New("queue not running")
	}

	if task == nil {
		return errors.New("task is nil")
	}

	go func() {
		// TODO: Check if this prevents writing to channel when queue is not running
		q.lock.Lock()
		defer q.lock.Unlock()

		if !q.running {
			return
		}

		q.queue <- &basicQueueTask{
			task:     task,
			callback: callback,
			retry:    0,
		}
	}()

	return nil
}
