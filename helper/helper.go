package helper

type OpenClose interface {
	Open() error
	Close() error
}
