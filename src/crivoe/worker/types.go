package worker

type WorkerOptions struct {
	// Task Id
	Id string `json:"id"`

	// Options for task
	Options interface{} `json:"options"`
}
