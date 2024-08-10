package pupupu_impl

import (
	"encoding/json"
)

const (
	KVSItemTypeUndefined = -1
	KVSItemTypeTask      = 0
	KVSItemTypeJob       = 1
)

type KVSItemType int

func KVSItemTypeToString(t KVSItemType) string {
	switch t {
	case KVSItemTypeTask:
		return "task"
	case KVSItemTypeJob:
		return "job"
	default:
		return "undefined"
	}
}

type KVSItem struct {
	// Type of record (Task or Job)
	Type KVSItemType `json:"kind"`

	// Id of node
	Id string `json:"id"`

	// Ids of child jobs
	JobIds []string `json:"job_ids"`

	// Status of Task
	Status string `json:"status"`
}

// Serializer for KVSItem
type KVSItemSerializer struct{}

func (s *KVSItemSerializer) Serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (w *KVSItemSerializer) Deserialize(bytes []byte) (interface{}, error) {
	var value KVSItem
	err := json.Unmarshal(bytes, &value)
	return &value, err
}

func NewKVSItemSerializer() *KVSItemSerializer {
	return &KVSItemSerializer{}
}

// Unsafe convert without type checks. Must be used with KVSItemSerializer
func UnsafeConvertToKVSItem(value interface{}) *KVSItem {
	kvsItem, ok := value.(*KVSItem)
	if !ok {
		panic("unexpected type")
	}

	return kvsItem
}
