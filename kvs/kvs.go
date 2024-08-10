package kvs

type KVSKeyIterator interface {
	Next() (string, bool)
}

// Type container for anonymous functions
type KVSKeyIteratorFunc func() (string, bool)

func (f KVSKeyIteratorFunc) Next() (string, bool) {
	return f()
}

type KVS interface {
	Set(key string, value interface{}) error
	Get(key string) (interface{}, error)
	Has(key string) (bool, error)
	Remove(key string) error
	List() ([]string, error)

	// Get iterator over keys
	//
	// In basic case there is no guarantee that storage supports calls to `Set()` while iterating
	KeyIterator() KVSKeyIterator
}
