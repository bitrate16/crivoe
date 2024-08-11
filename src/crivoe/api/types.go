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
	Id   string   `json:"id"`
	Jobs []string `json:"jobs"`
}

type HTTPJobSpec struct {
	Id string `json:"id"`
}

type HTTPJobStatus struct {
	Id     string `json:"id"`
	Status string `json:"status"`
}

type HTTPTaskStatus struct {
	Id     string           `json:"id"`
	Status string           `json:"status"`
	Jobs   []*HTTPJobStatus `json:"jobs"`
}

type HTTPTaskMetadata struct {
	Id       string      `json:"id"`
	Status   string      `json:"status"`
	Metadata interface{} `json:"metadata"`
}

type HTTPDeleted struct {
	Id string `json:"id"`
}

type HTTPTaskIdList struct {
	Tasks []string `json:"tasks"`
}
