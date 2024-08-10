package pupupu_impl

import (
	"crivoe/interfaces"
	"sync"
)

type wq_chain struct {
	next  *wq_chain
	value interface{}
}

type LinkedWaitQueue struct {
	lock  sync.Mutex
	cond  *sync.Cond
	first *wq_chain
	last  *wq_chain
	drop  bool
}

func NewLinkedWaitQueue() *LinkedWaitQueue {
	q := &LinkedWaitQueue{
		first: nil,
		last:  nil,
		drop:  false,
	}
	q.cond = sync.NewCond(&q.lock)

	return q
}

// Push item into Queue
// Returns `hasPushed` which may be `false` when queue is dropped
func (c *LinkedWaitQueue) Push(value interface{}) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.drop {
		return false
	}

	needUnblock := c.first == nil

	wq_chain := &wq_chain{
		value: value,
	}

	if c.first == nil {
		c.first = wq_chain
		c.last = wq_chain
	} else {
		c.last.next = wq_chain
		c.last = wq_chain
	}

	if needUnblock {
		c.cond.Broadcast()
	}

	return true
}

// Pop item from Queue
// Returns `(itemValue, hasItem)`
// `hasItem` is set to `false` only when queue is dropped
func (c *LinkedWaitQueue) Pop() (interface{}, bool) {
	c.lock.Lock()

	for c.first == nil && !c.drop {
		c.cond.Wait()
	}
	defer c.lock.Unlock()

	if c.drop {
		return nil, false
	}

	value := c.first.value
	c.first = c.first.next

	return value, true
}

// Drop everything from queue & unblock all WaitPop operations
func (c *LinkedWaitQueue) Cancel(sink interfaces.WaitQueueSink) {
	c.lock.Lock()
	defer c.lock.Unlock()

	needUnblock := c.first == nil

	c.drop = true
	for c.first != nil {
		value := c.first.value
		c.first = c.first.next

		sink.Handle(value)
	}
	c.last = nil

	if needUnblock {
		c.cond.Broadcast()
	}
}

// Drop everything from queue & unblock all WaitPop operations
func (c *LinkedWaitQueue) Reset() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.drop = false
}
