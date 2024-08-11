package pupupu_impl

import "sync"

type chain struct {
	next  *chain
	value interface{}
}

type LinkedQueue struct {
	lock  sync.Mutex
	first *chain
	last  *chain
	size  int
}

func NewLinkedQueue() *LinkedQueue {
	return &LinkedQueue{
		first: nil,
		last:  nil,
		size:  0,
	}
}

// Push item into Queue
func (c *LinkedQueue) Push(value interface{}) {
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
func (c *LinkedQueue) Pop() (interface{}, bool) {
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
func (c *LinkedQueue) Size() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.size
}
