package worker

import (
	"crivoe/pupupu"
	"fmt"
	"time"
)

const DEBUG_DELAY = time.Second * 1

type DebugWorker struct {
}

func (w *DebugWorker) Launch(task *pupupu.WorkerTask, master pupupu.Master, callback pupupu.Callback) {
	fmt.Printf("Got Task ID: %s with %d Jobs:\n", task.Id, len(task.Jobs))
	for index, job := range task.Jobs {
		fmt.Printf("[%d] Job ID: %s\n", index, job.Id)
		fmt.Printf("     Options: %+v\n", job.Job.Options)
	}

	fmt.Printf("sleep for %v\n", DEBUG_DELAY)
	time.Sleep(DEBUG_DELAY)

	for _, job := range task.Jobs {
		fmt.Printf("Mark COMPLETE Job ID: %s\n", job.Id)
		callback.JobCallback(&pupupu.JobResult{
			Status: pupupu.StatusComplete,
			Job:    job,
		})
	}

	fmt.Printf("Mark COMPLETE Task ID: %s\n", task.Id)
	callback.TaskCallback(&pupupu.TaskResult{
		Status: pupupu.StatusComplete,
		Task:   task,
	})
}
