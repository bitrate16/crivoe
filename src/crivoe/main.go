package main

import (
	"crivoe/api"
	"crivoe/config"
	"crivoe/pupupu"
	"crivoe/pupupu_impl"
	"crivoe/version"
	"crivoe/worker"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"golang.org/x/exp/rand"
)

var master *pupupu_impl.NailsMaster
var args *config.Config

// POST New scrape task
func create(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	var requestTask api.HTTPTask

	err := json.NewDecoder(req.Body).Decode(&requestTask)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	var task pupupu.Task
	task.Type = requestTask.Type
	task.MaxRetries = requestTask.MaxRetries
	task.Options = requestTask.Options
	task.Jobs = make([]*pupupu.Job, 0)

	for _, requestJob := range requestTask.Jobs {
		var job pupupu.Job
		job.Options = requestJob.Options

		task.Jobs = append(task.Jobs, &job)
	}

	taskSpec := master.Add(
		&task,
		pupupu.NewCallbackWrap(
			func(taskResult *pupupu.TaskResult) {
				if args.Debug {
					fmt.Printf("taskCallback(%+v)\n", taskResult)
				}

				if taskResult == nil {
					fmt.Printf("taskCallback(%+v): TaskResult is nil", taskResult)
					return
				}

				if taskResult.Task == nil {
					fmt.Printf("taskCallback(%+v): TaskResult.Task is nil", taskResult.Task)
					return
				}

				if args.Log {
					fmt.Printf("Task Done: %s\n", taskResult.Task.Id)
				}
			},
			func(jobResult *pupupu.JobResult) {
				if args.Debug {
					fmt.Printf("jobCallback(%+v)\n", jobResult)
				}

				if jobResult == nil {
					fmt.Printf("jobCallback(%+v): JobResult is nil", jobResult)
					return
				}

				if jobResult.Job == nil {
					fmt.Printf("jobCallback(%+v): JobResult.Job is nil", jobResult.Job)
					return
				}

				if args.Log {
					fmt.Printf("Job Done: %s\n", jobResult.Job.Id)
				}
			},
		),
	)

	if taskSpec == nil {
		http.Error(w, "Failed submit task", http.StatusInternalServerError)
		return
	}

	if args.Log {
		fmt.Printf("Task Created: %s\n", taskSpec.Id)
	}

	var taskSpecHTTP api.HTTPTaskSpec
	taskSpecHTTP.Id = taskSpec.Id
	taskSpecHTTP.JobIds = make([]string, 0)

	for _, jobSpec := range taskSpec.JobSpecs {
		taskSpecHTTP.JobIds = append(taskSpecHTTP.JobIds, jobSpec.Id)

		if args.Log {
			fmt.Printf("Job Created: %s\n", jobSpec.Id)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(taskSpecHTTP)
}

// GET Status of scrape task
func task_status(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	if !req.URL.Query().Has("id") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := req.URL.Query().Get("id")

	// Track state
	statusValue := master.GetTaskStatus(id)
	if statusValue == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	// Convert to API type
	var taskStatus api.HTTPTaskStatus
	taskStatus.Id = statusValue.Id
	taskStatus.Status = statusValue.Status
	taskStatus.Jobs = make([]*api.HTTPJobStatus, 0)

	for _, job := range statusValue.Jobs {
		var jobStatus api.HTTPJobStatus
		jobStatus.Id = job.Id
		jobStatus.Status = job.Status

		taskStatus.Jobs = append(taskStatus.Jobs, &jobStatus)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(taskStatus)
}

// GET Status of scrape task
func job_status(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	if !req.URL.Query().Has("id") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := req.URL.Query().Get("id")

	// Track state
	statusValue := master.GetJobStatus(id)
	if statusValue == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	// Convert to API type
	var jobStatus api.HTTPJobStatus
	jobStatus.Id = statusValue.Id
	jobStatus.Status = statusValue.Status

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jobStatus)
}

// GET Metadata of scrape result
func task_metadata(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	if !req.URL.Query().Has("id") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := req.URL.Query().Get("id")

	metadataValue := master.GetTaskMetadata(id)
	if metadataValue == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	var metadata api.HTTPTaskMetadata
	metadata.Id = metadataValue.Id
	metadata.Status = metadataValue.Status
	metadata.Metadata = metadataValue.Metadata

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(metadata)
}

// GET Metadata of scrape result
func job_metadata(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	if !req.URL.Query().Has("id") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := req.URL.Query().Get("id")

	metadataValue := master.GetJobMetadata(id)
	if metadataValue == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	var metadata api.HTTPTaskMetadata
	metadata.Id = metadataValue.Id
	metadata.Status = metadataValue.Status
	metadata.Metadata = metadataValue.Metadata

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(metadata)
}

// GET Metadata of scrape result
func job_data(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	if !req.URL.Query().Has("id") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := req.URL.Query().Get("id")

	reader := master.GetJobDataReader(id)
	if reader == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	stream, err := reader.GetReader()
	if err != nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	if closer, ok := stream.(io.Closer); ok {
		defer closer.Close()
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, stream)
	if err != nil {
		fmt.Printf("Write response error: %v\n", err)
		return
	}
}

func main() {
	rand.Seed(uint64(time.Now().UnixNano()))

	args = config.GetConfig()

	if args.Debug {
		fmt.Printf("Args: %+v\n", args)
	}

	if args.DisplayVersion {
		fmt.Printf("Version %s\n", version.VERSION)
		return
	}

	master = pupupu_impl.NewNailsMaster(args.StoragePath, args.MemoryMode, args.Debug)

	// Register workers
	worker.SetDebug(args.Debug)
	for key, value := range worker.GetRegistry() {
		master.RegisterWorker(key, value)
	}

	// Master lifecycle
	err := master.Start()
	if err != nil {
		fmt.Printf("No nails today: %v\n", err)
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		fmt.Println("Stopping")
		master.Stop()
		os.Exit(0)
	}()

	// Create router
	mux := http.NewServeMux()
	mux.HandleFunc("/create", create)
	mux.HandleFunc("/task_status", task_status)
	mux.HandleFunc("/job_status", job_status)
	mux.HandleFunc("/task_metadata", task_metadata)
	mux.HandleFunc("/job_metadata", job_metadata)
	mux.HandleFunc("/job_data", job_data)

	// Start it
	address := args.Host + ":" + strconv.Itoa(args.Port)
	fmt.Printf("Server listening on %s\n", address)
	err = http.ListenAndServe(address, mux)
	fmt.Printf("No uwu: %v\n", err)
}
