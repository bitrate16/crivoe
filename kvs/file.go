package kvs

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

type FileKVS struct {
	isOpen     bool
	path       string
	db         *sql.DB
	serializer Serializer
}

func (s *FileKVS) initDB() {
	s.db.Exec("create table if not exists kvs (key text primary key, value text)")
	// s.db.Exec("create index if not exists idx_kvs_key on metadata(key)")
}

func NewFileKVS(
	path string,
	serializer Serializer,
) *FileKVS {
	absPath, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}

	return &FileKVS{
		path:       absPath,
		isOpen:     false,
		serializer: serializer,
	}
}

// Open FileKVS in goroutine-safe mode
//
// WARNING:
//
//	This implementation supports only multigoroutine access, but not the multiprocess access. opening database in multiprocess mode will cause database corruption.
func (kvs *FileKVS) Open() error {
	if kvs.isOpen {
		return errors.New("kvs is open")
	}

	os.Mkdir(kvs.path, 0755)
	dbPath := filepath.Join(kvs.path, "kvs.db")
	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=rwc&nolock=1")
	if err != nil {
		return err
	}
	kvs.db = db
	kvs.initDB()

	kvs.isOpen = true

	return nil
}

func (kvs *FileKVS) Close() error {
	if !kvs.isOpen {
		return errors.New("kvs is closed")
	}

	err := kvs.db.Close()

	kvs.isOpen = false

	return err
}

func (kvs *FileKVS) Set(key string, value interface{}) error {
	if !kvs.isOpen {
		return errors.New("kvs is closed")
	}

	valueBytes, err := kvs.serializer.Serialize(value)
	if err != nil {
		if value == nil {
			_, err = kvs.db.Exec("insert into kvs (key, value) values (?, null) on conflict (key) do update set value = null", key)
		} else {
			return err
		}
	} else {
		var value = string(valueBytes)
		_, err = kvs.db.Exec("insert into kvs (key, value) values (?, ?) on conflict (key) do update set value = ?", key, value, value)
	}

	return err
}

func (kvs *FileKVS) Get(key string) (interface{}, error) {
	if !kvs.isOpen {
		return nil, errors.New("kvs is closed")
	}

	rows, err := kvs.db.Query("select value from kvs where key = ?", key)
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, nil
	}

	var resultValueJson sql.NullString

	err = rows.Scan(&resultValueJson)
	if err != nil {
		return nil, err
	}

	if resultValueJson.Valid {
		value, err := kvs.serializer.Deserialize([]byte(resultValueJson.String))
		return value, err
	}

	return nil, nil
}

func (kvs *FileKVS) Has(key string) (bool, error) {
	if !kvs.isOpen {
		return false, errors.New("kvs is closed")
	}

	rows, err := kvs.db.Query("select value from kvs where key = ?", key)
	if err != nil {
		return false, err
	}

	return rows.Next(), nil
}

func (kvs *FileKVS) Remove(key string) error {
	if !kvs.isOpen {
		return errors.New("kvs is closed")
	}

	_, err := kvs.db.Exec("delete from kvs where key = ?", key)
	return err
}

func (kvs *FileKVS) List() ([]string, error) {
	if !kvs.isOpen {
		return nil, errors.New("kvs is closed")
	}

	rows, err := kvs.db.Query("select key from kvs")
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0)
	for rows.Next() {
		var key string
		err := rows.Scan(&key)
		if err != nil {
			return nil, nil
		}

		keys = append(keys, key)
	}

	return keys, nil
}

func (kvs *FileKVS) KeyIterator() KVSKeyIterator {
	var offset uint64

	return KVSKeyIteratorFunc(
		func() (string, bool) {
			rows, err := kvs.db.Query("select key from kvs order by key limit 1 offset ?", offset)
			if err != nil {
				fmt.Printf("Uncaught error in iterator: %v\n", err)
				return "", false
			}

			if !rows.Next() {
				return "", false
			}

			var key string
			err = rows.Scan(&key)
			if err != nil {
				fmt.Printf("Uncaught error in iterator: %v\n", err)
				return "", false
			}

			offset += 1

			return key, true
		},
	)
}
