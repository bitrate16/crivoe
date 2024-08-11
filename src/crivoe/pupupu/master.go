package pupupu

import (
	"crivoe/kvs"

	"github.com/bitrate16/bloby"
)

type Master interface {
	Add(task *Task, callback Callback) *TaskSpec
	GetTaskStatus(id string) *TaskStatus
	GetJobStatus(id string) *JobStatus
	GetTaskMetadata(id string) *TaskMetadata
	GetJobMetadata(id string) *JobMetadata
	GetJobDataReader(id string) JobDataReader

	// Delete task & all related jobs records & data
	DeleteTask(id string) error

	ListTaskId() []string

	// Returns KVS of Master, must not be accessed by anyone except workers
	GetKVS() kvs.KVS

	// Returns Storage of Master, must not be accessed by anyone except workers
	GetStorage() bloby.Storage

	Start() error
	Stop() error

	GetWorker(workerType string) (Worker, error)
	RegisterWorker(workerType string, worker Worker) error
}
