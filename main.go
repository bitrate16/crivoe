package main

import (
	"crivoe/api"
	"crivoe/config"
	"crivoe/pupupu"
	"crivoe/pupupu_impl"
	"crivoe/worker"
	"encoding/json"
	"fmt"
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
// Sample:
// curl -vvv 127.0.0.1:8374/create --request POST --data '{"type":"url","options":{"url":"https://google.com","method":"GET"}}'
// curl -vvv 127.0.0.1:8374/create --request POST --data '{"type":"url","jobs":[ {"options": {"url":"https://google.com","method":"GET"} }, {"options": {"url":"https://yandex.ru","method":"GET"} } ]}'
// curl -vvv 127.0.0.1:8374/create --request POST --data '{"type":"debug","jobs":[ {"options": {"url":"https://google.com","method":"GET"} }, {"options": {"url":"https://yandex.ru","method":"GET"} } ]}'
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

				if args.Log {
					fmt.Printf("Task Done: %s\n", taskResult.Task.Id)
				}
			},
			func(jobResult *pupupu.JobResult) {
				if args.Debug {
					fmt.Printf("jobCallback(%+v)\n", jobResult)
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
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(taskSpecHTTP)
}

// // GET Status of scrape task
// // Sample:
// // curl -vvv --request GET 127.0.0.1:8374/status?id=
// func status(w http.ResponseWriter, req *http.Request) {
// 	if req.Method != "GET" {
// 		http.Error(w, "Method not supported", http.StatusBadRequest)
// 		return
// 	}

// 	if !req.URL.Query().Has("id") {
// 		http.Error(w, "Not Found", http.StatusNotFound)
// 		return
// 	}
// 	id := req.URL.Query().Get("id")

// 	fmt.Printf("%s Get status\n", id)

// 	// Track state
// 	statusValue, err := store.Get(id)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 	}

// 	if statusValue == nil {
// 		http.Error(w, "Not Found", http.StatusNotFound)
// 		return
// 	}

// 	status, ok := statusValue.(string)
// 	if !ok {
// 		fmt.Printf("Invalid status %v\n", status)
// 		http.Error(w, "Not Found", http.StatusNotFound)
// 		return
// 	}

// 	var response api.HTTPStatus
// 	response.Id = id
// 	response.Status = status

// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(http.StatusCreated)
// 	json.NewEncoder(w).Encode(response)
// }

// // GET Metadata of scrape result
// // Sample:
// // curl -vvv --request GET 127.0.0.1:8374/status?id=
// func metadata(w http.ResponseWriter, req *http.Request) {
// 	if req.Method != "GET" {
// 		http.Error(w, "Method not supported", http.StatusBadRequest)
// 		return
// 	}

// 	if !req.URL.Query().Has("id") {
// 		http.Error(w, "Not Found", http.StatusNotFound)
// 		return
// 	}
// 	id := req.URL.Query().Get("id")

// 	fmt.Printf("%s Get status\n", id)

// 	// Track state
// 	statusValue, err := store.Get(id)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 	}

// 	if statusValue == nil {
// 		http.Error(w, "Not Found", http.StatusNotFound)
// 		return
// 	}

// 	status, ok := statusValue.(string)
// 	if !ok {
// 		fmt.Printf("Invalid status %v\n", status)
// 		http.Error(w, "Not Found", http.StatusNotFound)
// 		return
// 	}

// 	var response api.HTTPStatus
// 	response.Id = id
// 	response.Status = status

// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(http.StatusCreated)
// 	json.NewEncoder(w).Encode(response)
// }

func main() {
	rand.Seed(uint64(time.Now().UnixNano()))

	args = config.GetConfig()

	if args.Debug {
		fmt.Printf("Args: %+v\n", args)
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
	// defer master.Stop()

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
	// mux.HandleFunc("/status", status)
	// mux.HandleFunc("/metadata", metadata)

	// Start it
	address := args.Host + ":" + strconv.Itoa(args.Port)
	fmt.Printf("Server listening on %s\n", address)
	err = http.ListenAndServe(address, mux)
	fmt.Printf("No uwu: %v\n", err)
}
