package kvs

import "encoding/json"

type Serializer interface {
	// Serialize object to bytes
	Serialize(value interface{}) ([]byte, error)

	// Deserialize object from bytes
	Deserialize(bytes []byte) (interface{}, error)
}

// Default Serializer implementations based on json marshal/unmarshal
type DefaultSerializer struct {
}

func (w *DefaultSerializer) Serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (w *DefaultSerializer) Deserialize(bytes []byte) (interface{}, error) {
	var value interface{}
	err := json.Unmarshal(bytes, &value)
	return value, err
}

func NewDefaultSerializer() *DefaultSerializer {
	return &DefaultSerializer{}
}
