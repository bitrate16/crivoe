package main

import (
	"crivoe/api"
	"crivoe/config"
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

var queue *scheduling.BasicQueue

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
	queue.Add(
		&scheduling.BasicTask{
			Type:       task.Type,
			RetryDelay: task.RetryDelay,
			MaxRetries: task.MaxRetries,
			Options:    task.Options,
			Id:         id,
		},
		scheduling.BasicCallbackFunc(func(result *scheduling.BasicCallbackResult) {
			fmt.Printf("%s Done: %+v\n", id, result)
		}),
	)

	var response api.HTTPJob
	response.Id = id

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func main() {
	rand.Seed(uint64(time.Now().UnixNano()))

	args := config.GetConfig()

	fmt.Printf("Args: %+v\n", args)

	// Create storaage
	storage, err := bloby.NewFileStorage(args.StoragePath)
	if err != nil {
		fmt.Printf("Failed open storage: %e", err)
		os.Exit(1)
	}

	err = storage.Open()
	if err != nil {
		fmt.Printf("Failed open storage: %e", err)
		os.Exit(1)
	}
	defer storage.Close()

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

	// Start it
	address := args.Host + ":" + strconv.Itoa(args.Port)
	fmt.Printf("Server listening on %s\n", address)
	http.ListenAndServe(address, mux)
}
