package worker

import (
	"crivoe/pupupu"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bitrate16/bloby"
)

type UrlWorker struct{}

func NewUrlWorker() *UrlWorker {
	return &UrlWorker{}
}

// Oneshot url downloader
func (w *UrlWorker) Launch(task *pupupu.WorkerTask, master pupupu.Master, callback pupupu.Callback) {

	// Just fire goroutine
	go func() {
		if DEBUG {
			fmt.Printf("Task %s started\n", task.Id)
		}

		var taskResult pupupu.TaskResult
		taskResult.Task = task

		delay := tryGetUInt64OrCastFloat64(task.Task.Options, "delay", 0)

	JobLoop:
		for index, job := range task.Jobs {
			if index != 0 && delay != 0 {
				time.Sleep(time.Duration(delay) * time.Millisecond)
			}

			if DEBUG {
				fmt.Printf("Job (%d / %d) %s started\n", index+1, len(task.Jobs), job.Id)
				fmt.Printf("Job (%d / %d) %s Options: %+v\n", index+1, len(task.Jobs), job.Id, job.Job.Options)
			}

			var jobResult pupupu.JobResult
			jobResult.Job = job

			method := tryGetString(job.Job.Options, "method", "GET")
			url := tryGetString(job.Job.Options, "url", "")
			timeout := tryGetUInt64OrCastFloat64(job.Job.Options, "timeout", 10000)
			headers := tryGetStringMap(job.Job.Options, "headers", make(map[string]interface{}))

			timestamp_start := time.Now()

			client := &http.Client{
				Timeout: time.Duration(timeout) * time.Millisecond,
			}

			req, err := http.NewRequest(method, url, nil)
			if err != nil {
				if DEBUG {
					fmt.Printf("Job (%d / %d) %s error: %v\n", index+1, len(task.Jobs), job.Id, err)
				}

				jobResult.Status = pupupu.StatusFail
				jobResult.Result = err
				callback.JobCallback(&jobResult)
				continue JobLoop
			}

			// Apply headers
			for header, value := range headers {
				if valueStr, ok := value.(string); ok {
					req.Header.Set(header, valueStr)
				}
			}

			// Apply Host header
			if value, ok := headers["Host"]; ok {
				if valueStr, ok := value.(string); ok {
					req.Host = valueStr
				}
			}

			resp, err := client.Do(req)
			if err != nil {
				if DEBUG {
					fmt.Printf("Job (%d / %d) %s error: %v\n", index+1, len(task.Jobs), job.Id, err)
				}

				jobResult.Status = pupupu.StatusFail
				jobResult.Result = err
				callback.JobCallback(&jobResult)
				continue JobLoop
			}
			defer resp.Body.Close()

			// Save headers
			responseHeaders := make(map[string][]string)
			for k, v := range resp.Header {
				responseHeaders[k] = v
			}

			timestamp_end := time.Now()

			// Create metadata
			metadata := make(map[string]interface{})
			metadata["id"] = job.Id
			metadata["options"] = job.Job.Options
			metadata["timestamp"] = timestamp_start.UnixMilli()
			metadata["duration"] = timestamp_end.UnixMilli() - timestamp_start.UnixMilli()
			metadata["headers"] = responseHeaders
			metadata["method"] = method
			metadata["url"] = url
			metadata["job_status"] = pupupu.StatusUndefined

			var node bloby.Node
			has := false

			if exists, err := master.GetStorage().ExistsByName(job.Id); exists {
				if err == nil {
					n, err := master.GetStorage().GetByReference(job.Id)
					if err == nil {
						has = true
						node = n
					}
				}
			}
			if !has {
				n, err := master.GetStorage().Create(
					job.Id,
					metadata,
				)

				if err != nil {
					if DEBUG {
						fmt.Printf("Job (%d / %d) %s error: %v\n", index+1, len(task.Jobs), job.Id, err)
					}

					jobResult.Status = pupupu.StatusFail
					jobResult.Result = err
					callback.JobCallback(&jobResult)
					continue JobLoop
				}

				node = n
			}

			if writable, ok := node.(bloby.Writable); ok {

				writer, err := writable.GetWriter()
				if err != nil {
					if DEBUG {
						fmt.Printf("Job (%d / %d) %s error: %v\n", index+1, len(task.Jobs), job.Id, err)
					}

					jobResult.Status = pupupu.StatusFail
					jobResult.Result = err
					callback.JobCallback(&jobResult)
					continue JobLoop
				}

				_, err = io.Copy(writer, resp.Body)
				if err != nil {
					if DEBUG {
						fmt.Printf("Job (%d / %d) %s error: %v\n", index+1, len(task.Jobs), job.Id, err)
					}

					jobResult.Status = pupupu.StatusFail
					jobResult.Result = err
					callback.JobCallback(&jobResult)
					continue JobLoop
				}

				// Update metadata
				if mutable, ok := node.(bloby.Mutable); ok {
					metadata["job_status"] = pupupu.StatusComplete
					metadata["status"] = resp.Status
					metadata["status_code"] = resp.StatusCode
					mutable.SetMetadata(metadata)
				}

				if closer, ok := writer.(io.Closer); ok {
					closer.Close()
				}

				if DEBUG {
					fmt.Printf("Job (%d / %d) %s completed\n", index+1, len(task.Jobs), job.Id)
				}

				jobResult.Status = pupupu.StatusComplete
				jobResult.Result = metadata
				callback.JobCallback(&jobResult)

			} else {
				jobResult.Status = pupupu.StatusFail
				jobResult.Result = errors.New("storage not writable")
				callback.JobCallback(&jobResult)
			}
		}

		if DEBUG {
			fmt.Printf("Task %s completed\n", task.Id)
		}

		taskResult.Status = pupupu.StatusComplete
		taskResult.Result = nil
		callback.TaskCallback(&taskResult)
	}()
}
