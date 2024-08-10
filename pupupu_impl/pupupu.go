package pupupu_impl

import (
	"crivoe/helper"
	"crivoe/kvs"
	"crivoe/pupupu"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/bitrate16/bloby"
)

type NailsMaster struct {
	done        chan struct{}
	lock        sync.Mutex
	isOpen      bool
	kvs         kvs.KVS
	storage     bloby.Storage
	queue       pupupu.WaitQueue
	workers     map[string]pupupu.Worker
	memoryMode  bool
	storagePath string
}

func NewNailsMaster(
	storagePath string,
	memoryMode bool,
) *NailsMaster {
	var kvsBackend kvs.KVS
	if memoryMode {
		kvsBackend = kvs.NewMemoryKVS()
	} else {
		kvsBackend = kvs.NewSyncFileKVS(storagePath)
	}

	storageBackend := bloby.NewFileStorage(storagePath)

	return &NailsMaster{
		kvs:         kvsBackend,
		storage:     storageBackend,
		queue:       NewLinkedWaitQueue(),
		workers:     make(map[string]pupupu.Worker),
		memoryMode:  memoryMode,
		storagePath: storagePath,
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
			taskObj, has := m.queue.WaitPop()
			if !has {
				fmt.Println("Queue dropped")
				m.done <- struct{}{}
				return
			}

			task, ok := taskObj.(*pupupu.WorkerTask)
			if !ok {
				fmt.Printf("Malformed type for %+v: %T\n", taskObj, taskObj)
				continue
			}

			// fmt.Printf("Task to dispatch: %+v\n", task)

			// Find worker for task
			worker, err := m.GetWorker(task.Task.Type)

			// Fail on noexistent worker
			if err != nil {
				task.Callback.TaskCallback(
					&pupupu.TaskResult{
						Status: pupupu.StatusFail,
						Result: err,
					},
				)
				continue
			}

			// Fire worker
			worker.Launch(
				task,
				m,
				pupupu.NewCallbackWrap(
					func(taskResult *pupupu.TaskResult) {
						if has, err := m.kvs.Has(taskResult.Task.Id); !has || (err != nil) {
							// Not existing
							fmt.Printf("Task %s not found in KVS\n", taskResult.Task.Id)

							// Notify upstream
							task.Callback.TaskCallback(taskResult)
							return
						}

						item, err := m.kvs.Get(taskResult.Task.Id)
						if err != nil {
							fmt.Printf("Get Task %s from KVS Error: %v\n", taskResult.Task.Id, err)

							// Notify upstream
							task.Callback.TaskCallback(taskResult)
							return
						}

						// TODO: Better type conversion
						kvsItem, err := ConvertToKVSItem(item)
						if err == nil {
							if kvsItem.Kind == KVSItemTypeTask {
								kvsItem.Status = taskResult.Status

								// Update status
								err := m.kvs.Set(taskResult.Task.Id, kvsItem)
								if err != nil {
									fmt.Printf("Set Task %s in KVS Error: %v\n", taskResult.Task.Id, err)
								}

								// Notify upstream
								task.Callback.TaskCallback(taskResult)
								return
							}

							// Not Task
							fmt.Printf("Task %s is a Job in KVS\n", taskResult.Task.Id)

							// Notify upstream
							task.Callback.TaskCallback(taskResult)
							return
						}

						// Not Task
						fmt.Printf("Task %s malformed type in KVS: %T, %v\n", taskResult.Task.Id, item, err)

						// Notify upstream
						task.Callback.TaskCallback(taskResult)
					},
					func(jobResult *pupupu.JobResult) {
						if has, err := m.kvs.Has(jobResult.Job.Id); !has || (err != nil) {
							// Not existing
							fmt.Printf("Job %s not found in KVS\n", jobResult.Job.Id)

							// Notify upstream
							task.Callback.JobCallback(jobResult)
							return
						}

						item, err := m.kvs.Get(jobResult.Job.Id)
						if err != nil {
							fmt.Printf("Get Job %s from KVS Error: %v\n", jobResult.Job.Id, err)

							// Notify upstream
							task.Callback.JobCallback(jobResult)
							return
						}

						// TODO: Better type conversion
						kvsItem, err := ConvertToKVSItem(item)
						if err == nil {
							if kvsItem.Kind == KVSItemTypeJob {
								kvsItem.Status = jobResult.Status

								// Update status
								err := m.kvs.Set(jobResult.Job.Id, kvsItem)
								if err != nil {
									fmt.Printf("Set Job %s in KVS Error: %v\n", jobResult.Job.Id, err)
								}

								// Notify upstream
								task.Callback.JobCallback(jobResult)
								return
							}

							// Not Task
							fmt.Printf("Job %s is a Task in KVS\n", jobResult.Job.Id)

							// Notify upstream
							task.Callback.JobCallback(jobResult)
							return
						}

						// Not Task
						fmt.Printf("Job %s malformed type in KVS: %T, %v\n", jobResult.Job.Id, item, err)

						// Notify upstream
						task.Callback.JobCallback(jobResult)
					},
				),
			)
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
	if kvsOpenClose, ok := m.kvs.(helper.OpenClose); ok {
		err := kvsOpenClose.Open()
		if err != nil {
			// Cascade back
			err2 := m.storage.Close()
			if err2 != nil {
				return errors.Join(err, err2)
			}
			return err
		}
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

	// Wait for finish
	m.queue.WaitDrop()
	<-m.done
	close(m.done)

	err1 := m.storage.Close()
	var err2 error
	if kvsOpenClose, ok := m.kvs.(helper.OpenClose); ok {
		err2 = kvsOpenClose.Close()
	}

	if err1 != nil {
		if err2 != nil {
			return errors.Join(err1, err2)
		}
		return err1
	} else if err2 != nil {
		return err2
	}

	// Drop storage
	if m.memoryMode {
		os.RemoveAll(m.storagePath)
	}

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
	taskSpec.JobSpecs = make([]*pupupu.JobSpec, 0, len(task.Jobs))

	// Create Worker Task
	var workerTask pupupu.WorkerTask
	workerTask.Id = taskSpec.Id
	workerTask.Task = task
	workerTask.Jobs = make([]*pupupu.WorkerJob, 0, len(task.Jobs))
	workerTask.Callback = callback

	// Create KVS Item
	var kvsTask KVSItem
	kvsTask.Kind = KVSItemTypeTask
	kvsTask.Status = pupupu.StatusUndefined
	kvsTask.JobIds = make([]string, 0, len(task.Jobs))
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
		workerJob.Callback = callback

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
		kvsTask.JobIds = append(kvsTask.JobIds, kvsJob.Id)

		// TODO: optimize multiple insertions into KVS
		// Push KVS Item
		m.kvs.Set(kvsJob.Id, kvsJob)
	}

	// Push KVS Item
	m.kvs.Set(kvsTask.Id, kvsTask)

	// Put Worker Task
	done := m.queue.WaitPush(&workerTask)
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

	if kvsItem, ok := item.(KVSItem); ok {
		if kvsItem.Kind == KVSItemTypeTask {
			return &pupupu.TaskStatus{
				Id:     kvsItem.Id,
				Status: kvsItem.Status,
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

	if kvsItem, ok := item.(KVSItem); ok {
		if kvsItem.Kind == KVSItemTypeJob {
			return &pupupu.JobStatus{
				Id:     kvsItem.Id,
				Status: kvsItem.Status,
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

	if kvsItem, ok := item.(KVSItem); ok {
		if kvsItem.Kind == KVSItemTypeTask {
			if kvsItem.Status == pupupu.StatusComplete {
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
					Id:       kvsItem.Id,
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

	if kvsItem, ok := item.(KVSItem); ok {
		if kvsItem.Kind == KVSItemTypeJob {
			if kvsItem.Status == pupupu.StatusComplete {
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
					Id:       kvsItem.Id,
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

	if kvsItem, ok := item.(KVSItem); ok {
		if kvsItem.Kind == KVSItemTypeJob {
			if kvsItem.Status == pupupu.StatusComplete {
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
