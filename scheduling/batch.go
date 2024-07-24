package scheduling

type Batch struct {
	// Array of tasks to process
	Tasks []Task `json:"tasks"`

	// Max retries per task
	MaxRetries int `json:"max_retries" default:1`

	// Max parallel tasks
	MaxParallel int `json:"max_parallel" default:1`

	// Delay between tasks in single parallel row
	TaskDelay int `json:"task_delay"`

	// Array of proxies to use
	Proxies []Proxy `json:"proxies"`

	// Array of header to use, overwrites default headers
	Headers []Header `json:"headers"`
}
