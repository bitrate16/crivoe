package bloby

type Storage interface {
	GetByReference(reference string) (Node, error)
	GetByName(name string) (Node, error)
	Create(name string) error
	Exists(name string) error
	List(namePrefix string, namePostfix string) ([]Node, error)
	ListReferences(namePrefix string, namePostfix string) ([]string, error)
	ListNames(namePrefix string, namePostfix string) ([]string, error)
	Open() error
	Close() error
}

type Node interface {
	GetReference() string
	GetName() string
	GetMetadata() interface{}
}
