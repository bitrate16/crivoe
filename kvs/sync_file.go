package kvs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type SyncFileKVS struct {
	lock   sync.RWMutex
	isOpen bool
	path   string
	db     *sql.DB
}

func (s *SyncFileKVS) initDB() {
	s.db.Exec("create table if not exists kvs (key text primary key, value text)")
	s.db.Exec("create index if not exists idx_kvs_key on metadata(key)")
}

func NewSyncFileKVS(path string) *SyncFileKVS {
	absPath, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}

	return &SyncFileKVS{
		path:   absPath,
		isOpen: false,
	}
}

func checkSyncFileKVSIsNil(kvs *SyncFileKVS) {
	if kvs == nil {
		panic("storage is nil")
	}
}

// Open FileKVS in goroutine-safe mode
//
// WARNING:
//
//	This implementation supports only multigoroutine access, but not the multiprocess access. opening database in multiprocess mode will cause database corruption.
func (kvs *SyncFileKVS) Open() error {
	checkSyncFileKVSIsNil(kvs)

	kvs.lock.RLock()
	defer kvs.lock.RUnlock()

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

func (kvs *SyncFileKVS) Close() error {
	checkSyncFileKVSIsNil(kvs)

	kvs.lock.RLock()
	defer kvs.lock.RUnlock()

	if !kvs.isOpen {
		return errors.New("kvs is closed")
	}

	err := kvs.db.Close()

	kvs.isOpen = false

	return err
}

func (kvs *SyncFileKVS) Set(key string, value interface{}) error {
	checkSyncFileKVSIsNil(kvs)

	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	if !kvs.isOpen {
		return errors.New("kvs is closed")
	}

	valueBytes, err := json.Marshal(value)
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

func (kvs *SyncFileKVS) Get(key string) (interface{}, error) {
	checkSyncFileKVSIsNil(kvs)

	kvs.lock.RLock()
	defer kvs.lock.RUnlock()

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
		var value interface{}
		err = json.Unmarshal([]byte(resultValueJson.String), &value)
		return value, err
	}

	return nil, nil
}

func (kvs *SyncFileKVS) Has(key string) (bool, error) {
	checkSyncFileKVSIsNil(kvs)

	kvs.lock.RLock()
	defer kvs.lock.RUnlock()

	if !kvs.isOpen {
		return false, errors.New("kvs is closed")
	}

	rows, err := kvs.db.Query("select value from kvs where key = ?", key)
	if err != nil {
		return false, err
	}

	return rows.Next(), nil
}

func (kvs *SyncFileKVS) Remove(key string) error {
	checkSyncFileKVSIsNil(kvs)

	kvs.lock.RLock()
	defer kvs.lock.RUnlock()

	if !kvs.isOpen {
		return errors.New("kvs is closed")
	}

	_, err := kvs.db.Exec("delete from kvs where key = ?", key)
	return err
}

func (kvs *SyncFileKVS) List() ([]string, error) {
	checkSyncFileKVSIsNil(kvs)

	kvs.lock.RLock()
	defer kvs.lock.RUnlock()

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
