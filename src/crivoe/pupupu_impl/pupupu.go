package pupupu_impl

import (
	"crivoe/interfaces"
	"crivoe/kvs"
	"crivoe/pupupu"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/bitrate16/bloby"
)

// Master implementation using in-memory or file-based storage for `KVS` and file-based storage for `Storage`.
//
// In memory mode, `Storage` is deleted on exit and no data persists.
// In regular mode, `KVS` and `Storage` are stored on disk.
//
// Backed of Queue is `LinkedQueue` which does not persist among restarts.
//
// Normal shutdown:
// * All tasks from Queue and related jobs are marked as cancelled.
// * In memory mode all Storage data is wiped from disk, KVS is deleted
// * In regular mode all Storage data and KVS data persist
//
// Emergency shutdown:
// * Queue drops
// * In memory mode KVS id deleted, Storage left on disk as-is
// * In regular mode KVS and Storage left on disk as-is
// * Restart in memory mode wipes all Storage from previous failure
// * restart in normal mode rewrites all task states that are not (completed, failed, cancelled) to cancelled state
type NailsMaster struct {
	done        chan struct{}
	lock        sync.Mutex
	isOpen      bool
	kvs         kvs.KVS
	storage     bloby.Storage
	queue       interfaces.WaitQueue
	workers     map[string]pupupu.Worker
	memoryMode  bool
	storagePath string
	debug       bool
}

func NewNailsMaster(
	storagePath string,
	memoryMode bool,
	debug bool,
) *NailsMaster {
	var kvsBackend kvs.KVS
	if memoryMode {
		kvsBackend = kvs.NewMemoryKVS()
	} else {
		kvsBackend = kvs.NewSyncFileKVS(storagePath, NewKVSItemSerializer())
	}

	storageBackend := bloby.NewFileStorage(storagePath)

	return &NailsMaster{
		kvs:         kvsBackend,
		storage:     storageBackend,
		queue:       NewLinkedWaitQueue(),
		workers:     make(map[string]pupupu.Worker),
		memoryMode:  memoryMode,
		storagePath: storagePath,
		debug:       debug,
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
			taskObj, has := m.queue.Pop()
			if !has {
				if m.debug {
					fmt.Println("Queue dropped")
				}
				m.done <- struct{}{}
				return
			}

			task, ok := taskObj.(*pupupu.WorkerTask)
			if !ok {
				fmt.Printf("Malformed type for %+v: %T\n", taskObj, taskObj)
				continue
			}

			if m.debug {
				fmt.Printf("Dispatching task to worker: %+v\n", task)
			}

			// Find worker for task
			worker, err := m.GetWorker(task.Task.Type)

			// Fail on noexistent worker
			if err != nil {
				task.Callback.TaskCallback(
					&pupupu.TaskResult{
						Status: pupupu.StatusFail,
						Result: err,
						Task:   task,
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
						if m.debug {
							fmt.Printf("Received TaskResult %+v\n", taskResult)
						}

						err := m.setStatusForId(taskResult.Task.Id, taskResult.Status, KVSItemTypeTask)
						if err != nil {
							fmt.Printf("%e\n", err)
						}

						// Notify upstream
						task.Callback.TaskCallback(taskResult)
					},
					func(jobResult *pupupu.JobResult) {
						if m.debug {
							fmt.Printf("Received JobResult %+v\n", jobResult)
						}

						err := m.setStatusForId(jobResult.Job.Id, jobResult.Status, KVSItemTypeJob)
						if err != nil {
							fmt.Printf("%e\n", err)
						}

						// Notify upstream
						task.Callback.JobCallback(jobResult)
					},
				),
			)
		}
	}()
}

// Validate integrity of KVS on emergency shutdown
//
// Rewrite all statuses on unfinished tasks to `StatusCancel`
func (m *NailsMaster) rewriteEntryStatusOnRestart() error {
	hasAlerted := false
	// var bar *progressbar.ProgressBar

	iterator := m.kvs.KeyIterator()
	for {
		key, has := iterator.Next()
		if !has {
			break
		}

		if !hasAlerted {
			hasAlerted = true
			fmt.Println("Validating integrity of KVS")

			// if m.debug {
			// 	bar = progressbar.Default(-1, "Validating integrity of KVS")
			// 	defer bar.Close()
			// }
		}
		// bar.Add(1)

		status, err := m.getStatusForId(key, KVSItemTypeUndefined)
		if err != nil {
			return err
		}

		if status != pupupu.StatusCancel &&
			status != pupupu.StatusComplete &&
			status != pupupu.StatusFail {

			if m.debug {
				fmt.Printf("Recover status for %s: %s\n", key, status)
			}
			err := m.setStatusForId(key, pupupu.StatusCancel, KVSItemTypeUndefined)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *NailsMaster) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isOpen {
		return errors.New("Master is open")
	}

	// Drop old storage
	if m.memoryMode {
		os.RemoveAll(m.storagePath)
	}

	// Open Storage
	err := m.storage.Open()
	if err != nil {
		return err
	}

	// Open KVS
	if kvsOpenClose, ok := m.kvs.(interfaces.OpenClose); ok {
		err := kvsOpenClose.Open()
		if err != nil {
			// Cascade close
			err2 := m.storage.Close()
			if err2 != nil {
				return errors.Join(err, err2)
			}
			return err
		}
	}

	// Try rewrite old statuses
	err = m.rewriteEntryStatusOnRestart()
	if err != nil {
		// Cascade close
		if kvsOpenClose, ok := m.kvs.(interfaces.OpenClose); ok {
			return errors.Join(err, kvsOpenClose.Open(), m.storage.Close())
		}
		return errors.Join(err, m.storage.Close())
	}

	m.queue.Reset()
	m.done = make(chan struct{}, 1)
	m.masterSession()

	m.isOpen = true

	// pupupu
	return nil
}

func (m *NailsMaster) getStatusForId(id string, expectedType KVSItemType) (string, error) {
	if has, err := m.kvs.Has(id); !has || (err != nil) {
		// Not existing
		return "", fmt.Errorf("Entry %s not found in KVS\n", id)
	}

	item, err := m.kvs.Get(id)
	if err != nil {
		return "", fmt.Errorf("Get Entry %s from KVS Error: %v\n", id, err)
	}

	kvsItem := UnsafeConvertToKVSItem(item)
	if expectedType != KVSItemTypeUndefined && kvsItem.Type != expectedType {
		return "", fmt.Errorf("Entity type %s does not match expected %s", KVSItemTypeToString(kvsItem.Type), KVSItemTypeToString(expectedType))
	}

	return kvsItem.Status, nil
}

func (m *NailsMaster) setStatusForId(id string, status string, expectedType KVSItemType) error {
	if has, err := m.kvs.Has(id); !has || (err != nil) {
		// Not existing
		return fmt.Errorf("Entry %s not found in KVS\n", id)
	}

	item, err := m.kvs.Get(id)
	if err != nil {
		return fmt.Errorf("Get Entry %s from KVS Error: %v\n", id, err)
	}

	kvsItem := UnsafeConvertToKVSItem(item)
	if expectedType != KVSItemTypeUndefined && kvsItem.Type != expectedType {
		return fmt.Errorf("Entity type %s does not match expected %s", KVSItemTypeToString(kvsItem.Type), KVSItemTypeToString(expectedType))
	}

	// Patch status
	kvsItem.Status = status
	if m.debug {
		fmt.Printf("Setting status of %s to %s\n", id, status)
	}

	// Update status
	err = m.kvs.Set(id, kvsItem)
	if err != nil {
		return fmt.Errorf("Set Entry %s in KVS Error: %v\n", id, err)
	}

	return nil
}

func (m *NailsMaster) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isOpen {
		return errors.New("Master is closed")
	}

	if m.debug {
		fmt.Printf("Cancelling queue\n")
	}

	// Drop queue & mark all tasks as cancelled
	m.queue.Cancel(
		interfaces.WaitQueueSinkFunc(
			func(value interface{}) {
				if m.debug {
					fmt.Printf("Finalizing cancelled task %+v\n", value)
				}

				task, ok := value.(*pupupu.WorkerTask)
				if !ok {
					fmt.Printf("Malformed type for %+v: %T\n", value, value)
					return
				}

				// Update status for task
				// TODO: Instead call task.Callback on each entry to delegate status processing to task callback
				err := m.setStatusForId(task.Id, pupupu.StatusCancel, KVSItemTypeUndefined)
				if err != nil {
					fmt.Printf("%e\n", err)
				}

				// Update status for each Job
				for _, job := range task.Jobs {
					// TODO: Instead call task.Callback on each entry to delegate status processing to task callback
					err := m.setStatusForId(job.Id, pupupu.StatusCancel, KVSItemTypeUndefined)
					if err != nil {
						fmt.Printf("%e\n", err)
					}
				}
			},
		),
	)

	if m.debug {
		fmt.Printf("Queue cancelled\n")
	}

	// Wait for finish
	<-m.done
	close(m.done)

	err1 := m.storage.Close()
	var err2 error
	if kvsOpenClose, ok := m.kvs.(interfaces.OpenClose); ok {
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
	kvsTask.Type = KVSItemTypeTask
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
		kvsJob.Type = KVSItemTypeJob
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
	done := m.queue.Push(&workerTask)
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

	kvsItem := UnsafeConvertToKVSItem(item)
	if kvsItem.Type == KVSItemTypeTask {
		var taskStatus pupupu.TaskStatus
		taskStatus.Id = kvsItem.Id
		taskStatus.Status = kvsItem.Status
		taskStatus.Jobs = make([]*pupupu.JobStatus, 0)

		for _, job := range kvsItem.JobIds {
			var jobStatus pupupu.JobStatus
			jobStatus.Id = job

			item, err := m.kvs.Get(job)
			if err != nil {
				fmt.Printf("GetTaskStatus Get Job Error: %v\n", err)
				jobStatus.Status = pupupu.StatusUndefined
			} else {
				kvsJobItem := UnsafeConvertToKVSItem(item)
				jobStatus.Status = kvsJobItem.Status
			}

			taskStatus.Jobs = append(taskStatus.Jobs, &jobStatus)
		}

		return &taskStatus
	}

	// Not Task
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

	kvsItem := UnsafeConvertToKVSItem(item)
	if kvsItem.Type == KVSItemTypeJob {
		return &pupupu.JobStatus{
			Id:     kvsItem.Id,
			Status: kvsItem.Status,
		}
	}

	// Not Job
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

	kvsItem := UnsafeConvertToKVSItem(item)
	if kvsItem.Type == KVSItemTypeTask {
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
				Status:   kvsItem.Status,
				Metadata: node.GetMetadata(),
			}
		}

		// Not completed
		return nil
	}

	// Not Task
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

	kvsItem := UnsafeConvertToKVSItem(item)
	if kvsItem.Type == KVSItemTypeJob {
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
				Status:   kvsItem.Status,
				Metadata: node.GetMetadata(),
			}
		}

		// Not completed
		return nil
	}

	// Not Job
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

	kvsItem := UnsafeConvertToKVSItem(item)
	if kvsItem.Type == KVSItemTypeJob {
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
