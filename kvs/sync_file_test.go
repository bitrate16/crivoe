package kvs

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncFileKVSCreate(t *testing.T) {
	testDirName := "test-TestSyncFileKVSCreate"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	kvs := NewSyncFileKVS(testDirName)
	assert.NotNil(t, kvs)

	err := kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)
}

func TestSyncFileKVSSet(t *testing.T) {
	testDirName := "test-TestSyncFileKVSSet"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	kvs := NewSyncFileKVS(testDirName)
	assert.NotNil(t, kvs)

	err := kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	// Add foo
	err = kvs.Set("foo", "value-foo")
	assert.NoError(t, err)

	// Existing foo
	has, err := kvs.Has("foo")
	assert.NoError(t, err)
	assert.True(t, has)

	// NotExisting bar
	has, err = kvs.Has("bar")
	assert.NoError(t, err)
	assert.False(t, has)

	// Add bar
	err = kvs.Set("bar", "value-bar")
	assert.NoError(t, err)

	// Existing foo
	has, err = kvs.Has("foo")
	assert.NoError(t, err)
	assert.True(t, has)

	// Existing bar
	has, err = kvs.Has("bar")
	assert.NoError(t, err)
	assert.True(t, has)

	// Add pat
	err = kvs.Set("pat", nil)
	assert.NoError(t, err)

	// Existing foo
	has, err = kvs.Has("foo")
	assert.NoError(t, err)
	assert.True(t, has)

	// Existing bar
	has, err = kvs.Has("bar")
	assert.NoError(t, err)
	assert.True(t, has)

	// Existing pat
	has, err = kvs.Has("pat")
	assert.NoError(t, err)
	assert.True(t, has)

	// Check values
	value, err := kvs.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "value-foo", value)

	value, err = kvs.Get("bar")
	assert.NoError(t, err)
	assert.Equal(t, "value-bar", value)

	value, err = kvs.Get("pat")
	assert.NoError(t, err)
	assert.Nil(t, value)

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)

	// Reopen & check persisting
	err = kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	// Existing foo
	has, err = kvs.Has("foo")
	assert.NoError(t, err)
	assert.True(t, has)

	// Existing bar
	has, err = kvs.Has("bar")
	assert.NoError(t, err)
	assert.True(t, has)

	// Existing pat
	has, err = kvs.Has("pat")
	assert.NoError(t, err)
	assert.True(t, has)

	// Check values
	value, err = kvs.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "value-foo", value)

	value, err = kvs.Get("bar")
	assert.NoError(t, err)
	assert.Equal(t, "value-bar", value)

	value, err = kvs.Get("pat")
	assert.NoError(t, err)
	assert.Nil(t, value)

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)
}

func TestSyncFileKVSSetRewrite(t *testing.T) {
	testDirName := "test-TestSyncFileKVSSetRewrite"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	kvs := NewSyncFileKVS(testDirName)
	assert.NotNil(t, kvs)

	err := kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	// Add foo
	err = kvs.Set("foo", "value-foo")
	assert.NoError(t, err)

	// Add bar
	err = kvs.Set("bar", "value-bar")
	assert.NoError(t, err)

	// Add pat
	err = kvs.Set("pat", nil)
	assert.NoError(t, err)

	// Rewrite
	err = kvs.Set("foo", "value-foo-new")
	assert.NoError(t, err)

	value, err := kvs.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "value-foo-new", value)

	err = kvs.Set("bar", nil)
	assert.NoError(t, err)

	value, err = kvs.Get("bar")
	assert.NoError(t, err)
	assert.Nil(t, value)

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)

	// Reopen & check persisting
	err = kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	value, err = kvs.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "value-foo-new", value)

	value, err = kvs.Get("bar")
	assert.NoError(t, err)
	assert.Nil(t, value)

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)
}

// Check a fully contains b with repeats
func checkFullContains(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	c := make([]bool, len(a), len(a))

	for ax := range len(a) {
		for bx := range len(a) {
			if !c[bx] && a[ax] == b[bx] {
				c[bx] = true
			}
		}
	}

	for index := range len(c) {
		if !c[index] {
			return false
		}
	}

	return true
}

func TestSyncFileKVSList(t *testing.T) {
	testDirName := "test-TestSyncFileKVSList"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	kvs := NewSyncFileKVS(testDirName)
	assert.NotNil(t, kvs)

	err := kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	keys := make([]string, 0)
	for index := range 100 {

		key := "key_" + strconv.Itoa(index)
		value := "value_" + strconv.Itoa(index)
		keys = append(keys, key)

		err = kvs.Set(key, value)
		assert.NoError(t, err)
	}

	keysKVS, err := kvs.List()
	assert.NoError(t, err)
	assert.True(t, checkFullContains(keys, keysKVS))

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)

	// Reopen & check persisting
	err = kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	keysKVS, err = kvs.List()
	assert.NoError(t, err)
	assert.True(t, checkFullContains(keys, keysKVS))

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)
}

func TestSyncFileKVSDelete(t *testing.T) {
	testDirName := "test-TestSyncFileKVSDelete"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	kvs := NewSyncFileKVS(testDirName)
	assert.NotNil(t, kvs)

	err := kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	keys := make([]string, 0)
	for index := range 100 {

		key := "key_" + strconv.Itoa(index)
		value := "value_" + strconv.Itoa(index)
		keys = append(keys, key)

		err = kvs.Set(key, value)
		assert.NoError(t, err)
	}

	keysKVS, err := kvs.List()
	assert.NoError(t, err)
	assert.True(t, checkFullContains(keys, keysKVS))

	for index := range 100 {
		err := kvs.Remove(keys[index])
		assert.NoError(t, err)
	}

	keysKVS, err = kvs.List()
	assert.NoError(t, err)
	assert.Empty(t, keysKVS)

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)

	// Reopen & check persisting
	err = kvs.Open()
	assert.NoError(t, err)
	assert.True(t, kvs.isOpen)

	keysKVS, err = kvs.List()
	assert.NoError(t, err)
	assert.Empty(t, keysKVS)

	err = kvs.Close()
	assert.NoError(t, err)
	assert.False(t, kvs.isOpen)
}
