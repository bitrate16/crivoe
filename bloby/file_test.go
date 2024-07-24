package bloby

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenClose(t *testing.T) {
	testDirName := "test-file-storage"

	t.Cleanup(func() {
		os.Remove(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}
