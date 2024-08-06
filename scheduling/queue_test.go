package scheduling

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper: stateless batch callback
type statelessBatchCallback struct {
	partial func(batchStatus *BatchStatus, taskStatus *TaskStatus)
	done    func(batchStatus *BatchStatus)
}

func (sbc *statelessBatchCallback) Partial(batchStatus *BatchStatus, taskStatus *TaskStatus) {
	sbc.partial(batchStatus, taskStatus)
}

func (sbc *statelessBatchCallback) Done(batchStatus *BatchStatus) {
	sbc.Done(batchStatus)
}

func TestQueueCreate(t *testing.T) {
	queue := NewQueue()
	assert.NotNil(t, queue)
	assert.False(t, queue.isRunning)

	err := queue.Start()
	assert.NoError(t, err)
	assert.True(t, queue.isRunning)

	err = queue.Stop()
	assert.NoError(t, err)
	assert.False(t, queue.isRunning)
}

func TestQueueDummyTask(t *testing.T) {
	queue := NewQueue()
	assert.NotNil(t, queue)
	assert.False(t, queue.isRunning)

	err := queue.Start()
	assert.NoError(t, err)
	assert.True(t, queue.isRunning)

	// Add workers
	RegisterWorkerType(
		"dummy",
		WorkerHandler(func(batch *Batch, task *Task, callback TaskCallback) {
			fmt.Printf("batch = %v, task = %v, callback = %v\n", batch, task, callback)
			callback.Done(&TaskStatus{
				Status: TaskStatusComplete,
				Result: "potato",
			})
		}),
	)

	sbc := statelessBatchCallback{
		partial: func(batchStatus *BatchStatus, taskStatus *TaskStatus) {
			fmt.Printf("Partial(%v, %v)\n", batchStatus, taskStatus)
		},
		done: func(batchStatus *BatchStatus) {
			fmt.Printf("Done(%v)\n", batchStatus)
		},
	}

	queue.AddBatch(
		&Batch{
			Tasks: []*Task{
				{
					Type: "dummy",
					Options: map[string]string{
						"name": "dummy",
					},
				},
				{
					Type: "unexpected",
					Options: map[string]string{
						"name": "unexpected",
					},
				},
			},
			MaxRetries: 3,
		},
		&sbc,
	)

	err = queue.Stop()
	assert.NoError(t, err)
	assert.False(t, queue.isRunning)
}
