package scheduling

import "sync"

type chain struct {
	next  *chain
	value *BasicTask
}

type BasicLinearTaskQueue struct {
	lock  sync.Mutex
	first *chain
	last  *chain
	size  int
}

func NewBasicLinearTaskQueue() *BasicLinearTaskQueue {
	return &BasicLinearTaskQueue{
		first: nil,
		last:  nil,
		size:  0,
	}
}

// Push item into Queue
func (c *BasicLinearTaskQueue) Push(value *BasicTask) {
	c.lock.Lock()
	defer c.lock.Unlock()

	chain := &chain{
		value: value,
	}

	if c.first == nil {
		c.first = chain
		c.last = chain
	} else {
		c.last.next = chain
		c.last = chain
	}

	c.size += 1
}

// Pop item from Queue
// Returns (itemValue, hasItem)
func (c *BasicLinearTaskQueue) Pop() (*BasicTask, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.first == nil {
		return nil, false
	} else {
		value := c.first.value
		c.first = c.first.next
		c.size -= 1
		return value, true
	}
}

// Get size of Queue
func (c *BasicLinearTaskQueue) Size() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.size
}
