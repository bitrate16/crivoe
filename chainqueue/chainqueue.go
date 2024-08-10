package chainqueue

import "sync"

type Item interface{}

type chain struct {
	next  *chain
	value Item
}

type ChainQueue struct {
	lock  sync.Mutex
	first *chain
	last  *chain
	size  int
}

func NewChainQueue() *ChainQueue {
	return &ChainQueue{
		first: nil,
		last:  nil,
		size:  0,
	}
}

// Push item into Queue
func (c *ChainQueue) Push(value Item) {
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
func (c *ChainQueue) Pop() (Item, bool) {
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
func (c *ChainQueue) Size() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.size
}
