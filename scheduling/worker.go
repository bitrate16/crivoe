package scheduling

import (
	"errors"
	"sync"
)

type Worker struct {
	lock      sync.Mutex
	quit      chan struct{}
	isRunning bool
	queue     *Queue
}

func NewWorker() *Worker {
	return &Worker{
		isRunning: false,
	}
}

func (w *Worker) workerLoop() {
	for {
		select {
		case <-w.quit:
			return
		case task := <-w.queue.queue:
			panic("not implemented")
		}
	}
}

func (w *Worker) Start() error {
	if w == nil {
		return errors.New("worker is nil")
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.isRunning {
		return errors.New("worker already running")
	}

	w.quit = make(chan struct{})
	w.isRunning = true
	go w.workerLoop()

	return nil
}

func (w *Worker) Stop() error {
	if w == nil {
		return errors.New("worker is nil")
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	if !w.isRunning {
		return errors.New("worker not running")
	}

	w.quit <- struct{}{}
	close(w.quit)
	w.isRunning = false

	return nil
}

func (w *Worker) setQueue(queue *Queue) error {
	if w == nil {
		return errors.New("worker is nil")
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.isRunning {
		return errors.New("can not change queue of running worker")
	}

	w.queue = queue

	return nil
}
