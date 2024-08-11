package kvs

import (
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type MemoryKVS struct {
	table map[string]interface{}
	lock  sync.RWMutex
}

func NewMemoryKVS() *MemoryKVS {
	return &MemoryKVS{
		table: make(map[string]interface{}),
	}
}

func (kvs *MemoryKVS) Set(key string, value interface{}) error {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	kvs.table[key] = value
	return nil
}

func (kvs *MemoryKVS) Get(key string) (interface{}, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	value, ok := kvs.table[key]

	if ok {
		return value, nil
	}

	return nil, fmt.Errorf("Key %s not found", key)
}

func (kvs *MemoryKVS) Has(key string) (bool, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	_, ok := kvs.table[key]
	return ok, nil
}

func (kvs *MemoryKVS) Remove(key string) error {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	_, ok := kvs.table[key]

	if ok {
		delete(kvs.table, key)
		return nil
	}

	return fmt.Errorf("Key %s not found", key)
}

func (kvs *MemoryKVS) List() ([]string, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	keys := make([]string, 0)

	for key := range kvs.table {
		keys = append(keys, key)
	}

	return keys, nil
}

func (kvs *MemoryKVS) KeyIterator() KVSKeyIterator {
	var offset uint64

	return KVSKeyIteratorFunc(
		func() (string, bool) {
			kvs.lock.Lock()
			defer kvs.lock.Unlock()

			// TODO: Better implementation of key seek
			var localOffset uint64
			for key := range kvs.table {
				if localOffset >= offset {
					offset += 1
					return key, true
				}
				localOffset += 1
			}

			return "", false
		},
	)
}
