package interfaces

type OpenClose interface {
	Open() error
	Close() error
}
