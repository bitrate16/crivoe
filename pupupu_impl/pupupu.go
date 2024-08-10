package pupupu_impl

import (
	"crivoe/kvs"
	"crivoe/pupupu"
	"errors"
	"fmt"
	"sync"

	"github.com/bitrate16/bloby"
)

type NailsMaster struct {
	done    chan struct{}
	lock    sync.Mutex
	isOpen  bool
	kvs     *kvs.FileKVS
	storage *bloby.FileStorage
	queue   pupupu.WaitQueue
	workers map[string]pupupu.Worker
}

func NewNailsMaster(
	storagepath string,
) *NailsMaster {
	return &NailsMaster{
		kvs:     kvs.NewFileKVS(storagepath),
		storage: bloby.NewFileStorage(storagepath),
		queue:   NewLinkedWaitQueue(),
	}
}

func (m *NailsMaster) GetKVS() kvs.KVS {
	return m.kvs
}

func (m *NailsMaster) GetStorage() bloby.Storage {
	return m.storage
}

// Register specific worker, not thread-safe
func (q *NailsMaster) RegisterWorker(workerType string, worker pupupu.Worker) error {
	if q == nil {
		return errors.New("queue is nil")
	}

	q.workers[workerType] = worker

	return nil
}

// Get specific worker, not thread-safe
func (q *NailsMaster) GetWorker(workerType string) (pupupu.Worker, error) {
	if q == nil {
		return nil, errors.New("queue is nil")
	}

	if worker, ok := q.workers[workerType]; ok {
		return worker, nil
	}

	return nil, fmt.Errorf("worker for %s does not exist", workerType)
}

// Main loop
func (m *NailsMaster) masterSession() {
	// Worker porker
	go func() {
		for {
			task, has := m.queue.WaitPop()
			if !has {
				fmt.Println("Queue dropped")
				m.done <- struct{}{}
				return
			}

			fmt.Printf("Task to dispatch: %+v\n", task)
		}
	}()
}

func (m *NailsMaster) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isOpen {
		return errors.New("Master is open")
	}

	// Open Storage
	err := m.storage.Open()
	if err != nil {
		return err
	}

	// Open KVS
	err = m.kvs.Open()
	if err != nil {
		// Cascade back
		err2 := m.storage.Close()
		if err2 != nil {
			return errors.Join(err, err2)
		}
		return err
	}

	m.queue.WaitReset()
	m.done = make(chan struct{}, 1)
	m.masterSession()

	m.isOpen = true

	// pupupu
	return nil
}

func (m *NailsMaster) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return errors.New("Master is closed")
	}

	err1 := m.storage.Close()
	err2 := m.kvs.Close()

	if err1 != nil {
		if err2 != nil {
			return errors.Join(err1, err2)
		}
		return err1
	} else if err2 != nil {
		return err2
	}

	// Wait for finish
	m.queue.WaitDrop()
	<-m.done
	close(m.done)

	m.isOpen = false

	// pupupu
	return nil
}

func (m *NailsMaster) Add(task *pupupu.Task, callback pupupu.Callback) *pupupu.TaskSpec {
	if task == nil {
		return nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return nil
	}

	// Create spec
	var taskSpec pupupu.TaskSpec
	taskSpec.Type = task.Type
	taskSpec.Id = RandStringRunes(NAILS_LENGTH)
	taskSpec.JobSpecs = make([]*pupupu.JobSpec, 0)

	// Create Worker Task
	var workerTask pupupu.WorkerTask
	workerTask.Id = taskSpec.Id
	workerTask.Task = task
	workerTask.Jobs = make([]*pupupu.WorkerJob, 0)

	// Create KVS Item
	var kvsTask KVSItem
	kvsTask.Kind = KVSItemTypeTask
	kvsTask.Status = pupupu.StatusUndefined
	kvsTask.JobIds = make([]string, 0)
	kvsTask.Id = taskSpec.Id

	// Map jobs
	for _, job := range task.Jobs {
		// Create Job Spec
		var jobSpec pupupu.JobSpec
		jobSpec.Id = RandStringRunes(NAILS_LENGTH)

		// Create Worker Job
		var workerJob pupupu.WorkerJob
		workerJob.Id = jobSpec.Id
		workerJob.Job = job

		// Create KVS Item
		var kvsJob KVSItem
		kvsJob.Kind = KVSItemTypeJob
		kvsJob.Id = jobSpec.Id
		kvsJob.Status = pupupu.StatusUndefined

		// Track spec
		taskSpec.JobSpecs = append(taskSpec.JobSpecs, &jobSpec)

		// Track Worker
		workerTask.Jobs = append(workerTask.Jobs, &workerJob)

		// Track KVS
		kvsTask.JobIds = append(kvsJob.JobIds, kvsJob.Id)

		// Push KVS Item
		m.kvs.Set(kvsJob.Id, kvsJob)
	}

	// Push KVS Item
	m.kvs.Set(kvsTask.Id, kvsTask)

	// Put Worker Task
	done := m.queue.WaitPush(workerTask)
	if !done {
		fmt.Println("Queue in unexpected dropped state")
		return nil
	}

	// Return spec
	return &taskSpec
}

func (m *NailsMaster) GetTaskStatus(id string) *pupupu.TaskStatus {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return nil
	}

	if has, err := m.kvs.Has(id); !has || (err != nil) {
		// Not existing
		return nil
	}

	item, err := m.kvs.Get(id)
	if err != nil {
		fmt.Printf("GetTaskStatus Error: %v\n", err)
		return nil
	}

	if kvmItem, ok := item.(KVSItem); ok {
		if kvmItem.Kind == KVSItemTypeTask {
			return &pupupu.TaskStatus{
				Id:     kvmItem.Id,
				Status: kvmItem.Status,
			}
		}

		// Not Task
		return nil
	}

	fmt.Printf("GetTaskStatus Error: invalid type %T\n", item)
	return nil
}

func (m *NailsMaster) GetJobStatus(id string) *pupupu.JobStatus {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return nil
	}

	if has, err := m.kvs.Has(id); !has || (err != nil) {
		// Not existing
		return nil
	}

	item, err := m.kvs.Get(id)
	if err != nil {
		fmt.Printf("GetJobStatus Error: %v\n", err)
		return nil
	}

	if kvmItem, ok := item.(KVSItem); ok {
		if kvmItem.Kind == KVSItemTypeJob {
			return &pupupu.JobStatus{
				Id:     kvmItem.Id,
				Status: kvmItem.Status,
			}
		}

		// Not Job
		return nil
	}

	fmt.Printf("GetJobStatus Error: invalid type %T\n", item)
	return nil
}

func (m *NailsMaster) GetTaskMetadata(id string) *pupupu.TaskMetadata {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return nil
	}

	if has, err := m.kvs.Has(id); !has || (err != nil) {
		// Not existing
		return nil
	}

	item, err := m.kvs.Get(id)
	if err != nil {
		fmt.Printf("GetTaskMetadata Error: %v\n", err)
		return nil
	}

	if kvmItem, ok := item.(KVSItem); ok {
		if kvmItem.Kind == KVSItemTypeTask {
			if kvmItem.Status == pupupu.StatusComplete {
				// Query node from storage
				node, err := m.storage.GetByName(id)
				if err != nil {
					fmt.Printf("GetTaskMetadata Error: %v\n", err)
					return nil
				}

				if node == nil {
					return nil
				}

				return &pupupu.TaskMetadata{
					Id:       kvmItem.Id,
					Metadata: node.GetMetadata(),
				}
			}

			// Not completed
			return nil
		}

		// Not Task
		return nil
	}

	fmt.Printf("GetTaskMetadata Error: invalid type %T\n", item)
	return nil
}

func (m *NailsMaster) GetJobMetadata(id string) *pupupu.JobMetadata {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return nil
	}

	if has, err := m.kvs.Has(id); !has || (err != nil) {
		// Not existing
		return nil
	}

	item, err := m.kvs.Get(id)
	if err != nil {
		fmt.Printf("GetJobMetadata Error: %v\n", err)
		return nil
	}

	if kvmItem, ok := item.(KVSItem); ok {
		if kvmItem.Kind == KVSItemTypeJob {
			if kvmItem.Status == pupupu.StatusComplete {
				// Query node from storage
				node, err := m.storage.GetByName(id)
				if err != nil {
					fmt.Printf("GetJobMetadata Error: %v\n", err)
					return nil
				}

				if node == nil {
					return nil
				}

				return &pupupu.JobMetadata{
					Id:       kvmItem.Id,
					Metadata: node.GetMetadata(),
				}
			}

			// Not completed
			return nil
		}

		// Not Job
		return nil
	}

	fmt.Printf("GetJobMetadata Error: invalid type %T\n", item)
	return nil
}

func (m *NailsMaster) GetJobDataReader(id string) pupupu.JobDataReader {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return nil
	}

	if has, err := m.kvs.Has(id); !has || (err != nil) {
		// Not existing
		return nil
	}

	item, err := m.kvs.Get(id)
	if err != nil {
		fmt.Printf("GetJobMetadata Error: %v\n", err)
		return nil
	}

	if kvmItem, ok := item.(KVSItem); ok {
		if kvmItem.Kind == KVSItemTypeJob {
			if kvmItem.Status == pupupu.StatusComplete {
				// Query node from storage
				node, err := m.storage.GetByName(id)
				if err != nil {
					fmt.Printf("GetJobMetadata Error: %v\n", err)
					return nil
				}

				if node == nil {
					return nil
				}

				if readableNode, ok := node.(bloby.Readable); ok {
					return pupupu.JobDataReaderFunc(readableNode.GetReader)
				}

				// Not readable
				return nil
			}

			// Not completed
			return nil
		}

		// Not Job
		return nil
	}

	fmt.Printf("GetJobMetadata Error: invalid type %T\n", item)
	return nil
}
