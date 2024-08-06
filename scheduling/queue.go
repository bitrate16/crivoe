package scheduling

import (
	"crivoe/config"
	"errors"
	"sync"
	"time"
)

type inputTask struct {
	task *Task
}

type inputBatch struct {
	batch       *Batch
	batchStatus *BatchStatus
}

type inputQueueItem struct {
	task     *inputTask
	batch    *inputBatch
	callback BatchCallback

	// Tetry attempt counter. Expecting task processing is sequential and one task can not be duplicated
	retry int
}

type Queue struct {
	lock      sync.Mutex
	quit      chan struct{}
	isRunning bool

	// Sink for tasks
	queue chan *inputQueueItem
}

func (q *Queue) queueLoop() {
	for {
		select {
		case <-q.quit:
			return
		case task := <-q.queue:
			// Find worker for task
			worker, err := GetWorkerForType(task.task.task.Type)
			if err != nil {
				task.callback.Partial(
					task.batch.batchStatus,
					&TaskStatus{
						Status: TaskStatusError,
						Result: err,
					},
				)
				continue
			}

			// Fire task
			worker.Launch(
				task.batch.batch,
				task.task.task,
				TaskCallbackHandler(func(status *TaskStatus) {
					if status.Status != TaskStatusError {
						task.callback.Partial(task.batch.batchStatus, status)
						return
					}

					if task.retry < task.batch.batch.MaxRetries {
						// Notify of error
						task.callback.Partial(task.batch.batchStatus, status)

						task.retry += 1

						// Reschedule somewhere in the future to prevent blocking
						go func() {
							// Wait before reschedule
							time.Sleep(time.Duration(task.batch.batch.TaskDelay) * time.Millisecond)

							// TODO: Check if this prevents writing to channel when queue is not running
							q.lock.Lock()
							defer q.lock.Unlock()

							if !q.isRunning {
								return
							}

							q.queue <- task
						}()
					} else {
						// Notify of failure, rewrite status
						status.Status = TaskStatusFail
						task.callback.Partial(task.batch.batchStatus, status)
					}
				}),
			)
		}
	}
}

func NewQueue() *Queue {
	return &Queue{
		quit:      make(chan struct{}),
		queue:     make(chan *inputQueueItem, config.GetConfig().MaxTasks),
		isRunning: false,
	}
}

func (q *Queue) Start() error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	if q.isRunning {
		return errors.New("queue already running")
	}

	q.quit = make(chan struct{})
	q.isRunning = true
	go q.queueLoop()

	return nil
}

func (q *Queue) Stop() error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	if !q.isRunning {
		return errors.New("queue not running")
	}

	q.quit <- struct{}{}
	close(q.quit)
	q.isRunning = false

	return nil
}

func (q *Queue) AddBatch(batch *Batch, callback BatchCallback) error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	if !q.isRunning {
		return errors.New("queue not running")
	}

	if batch == nil {
		return errors.New("batch is nil")
	}

	if len(batch.Tasks) == 0 {
		return nil
	}

	go func() {
		for _, task := range batch.Tasks {
			localBatch := inputBatch{
				batch: batch,
				batchStatus: &BatchStatus{
					Status: BatchStatusUndefined,
					Result: nil,
				},
			}

			localTask := inputTask{
				task: task,
			}

			// Wait before schedule
			time.Sleep(time.Duration(localBatch.batch.TaskDelay) * time.Millisecond)

			// TODO: Check if this prevents writing to channel when queue is not running
			q.lock.Lock()
			defer q.lock.Unlock()

			if !q.isRunning {
				return
			}

			q.queue <- &inputQueueItem{
				task:     &localTask,
				batch:    &localBatch,
				callback: callback,
				retry:    0,
			}
		}
	}()

	return nil
}
