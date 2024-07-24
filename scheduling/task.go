package scheduling

type Task struct {
	Url string `json:"url"`
}

// func NewTask() *Task {
// 	return &Task{}
// }

// func SerializeTask(task *Task) ([]byte, error) {
// 	return json.Marshal(*task)
// }

// func DeserializeTask(data []byte) (*Task, error) {
// 	var task Task
// 	err := json.Unmarshal(data, &task)
// 	return &task, err
// }
