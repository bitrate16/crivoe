package pupupu_impl

const (
	KVSItemTypeTask = 0
	KVSItemTypeJob  = 1
)

type KVSItem struct {
	// Kind of record (Task or Job)
	Kind int `json:"kind"`

	// Id of node
	Id string `json:"id"`

	// Ids of child jobs
	JobIds []string `json:"job_ids"`

	// Status of Task
	Status string `json:"status"`
}
