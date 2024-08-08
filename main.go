package main

import (
	"crivoe/api"
	"crivoe/config"
	"crivoe/kvs"
	"crivoe/scheduling"
	"crivoe/worker"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bitrate16/bloby"
	"golang.org/x/exp/rand"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

var store kvs.KVS
var storage bloby.Storage
var queue *scheduling.BasicQueue

// POST New scrape task
// Sample:
// curl -vvv 127.0.0.1:8374/create --request POST --data '{"type":"url","options":{"url":"https://google.com","method":"GET"}}'
func create(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	var task api.HTTPTask

	err := json.NewDecoder(req.Body).Decode(&task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	id := RandStringRunes(64)

	fmt.Printf("%s Create for %+v\n", id, task)

	// Track state
	store.Set(id, scheduling.BasicCallbackStatusUndefined)

	// Push to queue
	queue.Add(
		&scheduling.BasicTask{
			Type:       task.Type,
			RetryDelay: task.RetryDelay,
			MaxRetries: task.MaxRetries,
			Options: &worker.WorkerOptions{
				Id:      id,
				Options: task.Options,
			},
		},
		scheduling.BasicCallbackFunc(func(result *scheduling.BasicCallbackResult) {
			fmt.Printf("%s Done: %+v\n", id, result)
			store.Set(id, result.Status)
		}),
	)

	var response api.HTTPJob
	// TODO: Set to array based on all tasks being processed
	// TODO: Aff Worker.PrepareTask for task preparation & extruding into list
	response.Id = id

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// GET Status of scrape task
// Sample:
// curl -vvv --request GET 127.0.0.1:8374/status?id=
func status(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	if !req.URL.Query().Has("id") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := req.URL.Query().Get("id")

	fmt.Printf("%s Get status\n", id)

	// Track state
	statusValue, err := store.Get(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if statusValue == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	status, ok := statusValue.(string)
	if !ok {
		fmt.Printf("Invalid status %v\n", status)
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	var response api.HTTPStatus
	response.Id = id
	response.Status = status

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// GET Metadata of scrape result
// Sample:
// curl -vvv --request GET 127.0.0.1:8374/status?id=
func metadata(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(w, "Method not supported", http.StatusBadRequest)
		return
	}

	if !req.URL.Query().Has("id") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := req.URL.Query().Get("id")

	fmt.Printf("%s Get status\n", id)

	// Track state
	statusValue, err := store.Get(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if statusValue == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	status, ok := statusValue.(string)
	if !ok {
		fmt.Printf("Invalid status %v\n", status)
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	var response api.HTTPStatus
	response.Id = id
	response.Status = status

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func main() {
	rand.Seed(uint64(time.Now().UnixNano()))

	args := config.GetConfig()

	fmt.Printf("Args: %+v\n", args)

	// Create storaage
	s, err := bloby.NewFileStorage(args.StoragePath)
	if err != nil {
		fmt.Printf("Failed open storage: %e", err)
		os.Exit(1)
	}

	err = s.Open()
	if err != nil {
		fmt.Printf("Failed open storage: %e", err)
		os.Exit(1)
	}
	defer s.Close()
	storage = s

	// Create KVS
	kv, err := kvs.NewFileKVS(args.StoragePath)
	if err != nil {
		fmt.Printf("Failed open kvs: %e", err)
		os.Exit(1)
	}

	err = kv.Open()
	if err != nil {
		fmt.Printf("Failed open kvs: %e", err)
		os.Exit(1)
	}
	defer kv.Close()
	store = kv

	// Create newQueue
	newQueue := scheduling.NewBasicQueue()
	queue = newQueue

	// Add workers
	newQueue.RegisterWorker(
		"url",
		worker.NewUrlWorker(
			storage,
		),
	)

	// Queue lifecycle
	newQueue.Start()
	defer newQueue.Stop()

	// Create router
	mux := http.NewServeMux()
	mux.HandleFunc("/create", create)
	mux.HandleFunc("/status", status)

	// Start it
	address := args.Host + ":" + strconv.Itoa(args.Port)
	fmt.Printf("Server listening on %s\n", address)
	http.ListenAndServe(address, mux)
}
