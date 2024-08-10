package interfaces

// Handler for dropped queue values
type WaitQueueSink interface {
	Handle(value interface{})
}

// Type wrapper for anonymous functions
type WaitQueueSinkFunc func(value interface{})

func (w WaitQueueSinkFunc) Handle(value interface{}) {
	w(value)
}

// WaitQueue - Queue that supports waiting for values.
//
// WaitQueue must be started with `Reset()` in order to enable `Push`
// and `Pop` operations, else both operations will return `false`.
//
// WaitQueue can be stopped with `Cancel(WaitQueueSink)`. In this case,
// queue `Push` and `Pop` operations get frozen and return `false`; all
// awaiting `Pop` callers are unblocked with `false` status. All resting
// items from queue are passed to `WaitQueueSink` preserving  queue order.
type WaitQueue interface {
	Push(value interface{}) bool
	Pop() (interface{}, bool)
	Cancel(sink WaitQueueSink)
	Reset()
}
