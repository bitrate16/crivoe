package scheduling

import (
	"crivoe/kvs"
	"errors"
	"fmt"
	"sync"

	"github.com/bitrate16/bloby"
)

type LinearProcessor struct {
	queue   *BasicLinearTaskQueue
	kv      *kvs.KVS
	storage *bloby.Storage
	quit    chan struct{}
	lock    sync.Mutex
	workers map[string]BasicWorker
}

func NewLinearProcessor(kv *kvs.KVS, storage *bloby.Storage) *LinearProcessor {
	return &LinearProcessor{
		kv:      kv,
		storage: storage,
		queue:   NewBasicLinearTaskQueue(),
	}
}

// Register specific worker, not thread-safe
func (q *LinearProcessor) RegisterWorker(workerType string, worker BasicWorker) error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.workers[workerType] = worker

	return nil
}

// Get specific worker, not thread-safe
func (q *LinearProcessor) GetWorker(workerType string) (BasicWorker, error) {
	if q == nil {
		return nil, errors.New("queue is nil")
	}

	if worker, ok := q.workers[workerType]; ok {
		return worker, nil
	}

	return nil, fmt.Errorf("worker for %s does not exist", workerType)
}
