package pupupu

type Queue interface {
	// Must add item to end of queue
	Push(value interface{})

	// Must return `(itemValue, itemExists)`
	Pop() (interface{}, bool)

	// Must return size of queue
	Size() int
}
