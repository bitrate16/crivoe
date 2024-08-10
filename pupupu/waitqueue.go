package pupupu

// Handler for dropped queue values
type WaitQueueSink interface {
	Handle(value interface{})
}

// Type wrapper for anonymous functions
type WaitQueueSinkFunc func(value interface{})

func (w WaitQueueSinkFunc) Handle(value interface{}) {
	w(value)
}

type WaitQueue interface {
	// Add item to queue
	// Unblocks WaitPop if it was blocked
	WaitPush(value interface{}) bool

	// Wait for item in queue
	// Unblocks when `Size() > 0`
	WaitPop() (interface{}, bool)

	// Drop everything from queue & unblock all WaitPop operations
	// Each element of queue is passed into sink and queue is marked as dropped, requiring call to `Reset()`
	// After call to `Drop()` every task calling `WaitPop()` must receive `false` as `bool` indicator.
	// Call to `Reset()` must fix queue behavior to original state
	WaitDrop(sink WaitQueueSink)

	// Reser state Drop
	WaitReset()
}
