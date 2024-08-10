package pupupu

import "io"

// Job data reader returned to user
type JobDataReader interface {
	GetReader() (io.Reader, error)
}

// Type implementation for anonymout function
type JobDataReaderFunc func() (io.Reader, error)

func (f JobDataReaderFunc) GetReader() (io.Reader, error) {
	return f()
}
