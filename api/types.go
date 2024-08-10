package api

type HTTPJob struct {
	// Options fot the task
	Options interface{} `json:"options"`
}

type HTTPTask struct {
	// Worker type for this task
	Type string `json:"type"`

	// Max retries of the task
	MaxRetries int `json:"max_retries"`

	// Options fot the task
	Options interface{} `json:"options"`

	// Options fot the task
	Jobs []*HTTPJob `json:"jobs"`
}

type HTTPTaskSpec struct {
	Id     string   `json:"id"`
	JobIds []string `json:"job_ids"`
}

type HTTPJobSpec struct {
	Id string `json:"id"`
}

type HTTPStatus struct {
	Id     string `json:"id"`
	Status string `json:"status"`
}
