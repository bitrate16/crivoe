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

	// Returns KVS of Master, must not be accessed by anyone except workers
	GetKVS() kvs.KVS

	// Returns Storage of Master, must not be accessed by anyone except workers
	GetStorage() bloby.Storage

	Start() error
	Stop() error

	GetWorker(workerType string) (Worker, error)
	RegisterWorker(workerTyccpe string) error
}
