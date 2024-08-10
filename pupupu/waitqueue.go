package pupupu

type WaitQueue interface {
	// Add item to queue
	// Unblocks WaitPop if it was blocked
	WaitPush(value interface{}) bool

	// Wait for item in queue
	// Unblocks when `Size() > 0`
	WaitPop() (interface{}, bool)

	// Drop everything from queue & unblock all WaitPop operations
	WaitDrop()

	// Reser state Drop
	WaitReset()
}
