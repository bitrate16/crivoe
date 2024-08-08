package kvs

type KVS interface {
	Set(key string, value interface{}) error
	Get(key string) (interface{}, error)
	Has(key string) (bool, error)
	Remove(key string) error
	List() ([]string, error)
}
