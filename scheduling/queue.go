package scheduling

import (
	"crivoe/config"
	"errors"
	"sync"
)

type inputQueueTask struct {
	task  *Task
	batch *Batch
}

type Queue struct {
	lock      sync.Mutex
	quit      chan struct{}
	isRunning bool

	// Sink for tasks
	queue chan inputQueueTask
}

func (w *Queue) queueLoop() {
	for {
		select {
		case <-w.quit:
			return
		}
	}
}

func NewQueue() *Queue {
	return &Queue{
		quit:      make(chan struct{}),
		queue:     make(chan inputQueueTask, config.GetConfig().MaxTasks),
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

func (q *Queue) AddBatch(batch *Batch) error {
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

	for _, task := range batch.Tasks {
		q.queue <- inputQueueTask{
			task:  &task,
			batch: batch,
		}
	}

	return nil
}
