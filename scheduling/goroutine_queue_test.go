package scheduling

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Helper: stateless basic callback
type statelessBasicCallback struct {
	done func(result *BasicCallbackResult)
}

func (sbc *statelessBasicCallback) Done(result *BasicCallbackResult) {
	sbc.done(result)
}

func TestBasicQueueCreate(t *testing.T) {
	queue := NewBasicQueue()
	assert.NotNil(t, queue)
	assert.False(t, queue.running)

	err := queue.Start()
	assert.NoError(t, err)
	assert.True(t, queue.running)

	err = queue.Stop()
	assert.NoError(t, err)
	assert.False(t, queue.running)
}

func TestBasicQueueDummyTask(t *testing.T) {
	queue := NewBasicQueue()
	assert.NotNil(t, queue)
	assert.False(t, queue.running)

	err := queue.Start()
	assert.NoError(t, err)
	assert.True(t, queue.running)

	// Add workers
	queue.RegisterWorker(
		"dummy",
		BasicWorkerHandler(func(task *BasicTask, callback BasicCallback) {
			fmt.Printf("task = %v, callback = %v\n", task, callback)
			callback.Done(&BasicCallbackResult{
				Status: BasicCallbackStatusComplete,
				Result: "potato",
			})
		}),
	)

	sbc := statelessBasicCallback{
		done: func(result *BasicCallbackResult) {
			fmt.Printf("Done(%v)\n", result)
		},
	}

	queue.Add(
		&BasicTask{
			Type:       "dummy",
			MaxRetries: 3,
		},
		&sbc,
	)

	err = queue.Stop()
	assert.NoError(t, err)
	assert.False(t, queue.running)
}

func TestBasicQueueLongTask(t *testing.T) {
	queue := NewBasicQueue()
	assert.NotNil(t, queue)
	assert.False(t, queue.running)

	err := queue.Start()
	assert.NoError(t, err)
	assert.True(t, queue.running)

	// Add workers
	queue.RegisterWorker(
		"dummy",
		BasicWorkerHandler(func(task *BasicTask, callback BasicCallback) {
			fmt.Printf("task = %v, callback = %v\n", task, callback)
			callback.Done(&BasicCallbackResult{
				Status: BasicCallbackStatusComplete,
				Result: "potato",
			})
		}),
	)

	// Task must flip the flag
	var flag atomic.Bool
	flag.Store(false)

	sbc := statelessBasicCallback{
		done: func(result *BasicCallbackResult) {
			time.Sleep(time.Second * 1)
			fmt.Printf("Done(%v)\n", result)
			flag.Store(true)
		},
	}

	queue.Add(
		&BasicTask{
			Type:       "dummy",
			MaxRetries: 3,
		},
		&sbc,
	)

	time.Sleep(time.Second * 1)

	err = queue.Stop()
	assert.NoError(t, err)
	assert.False(t, queue.running)

	time.Sleep(time.Second * 2)

	// Expect task to finish
	assert.True(t, flag.Load())

}
