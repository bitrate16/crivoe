package bloby

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type FileStorage struct {
	lock sync.RWMutex
	db   *sql.DB
}

func (s *FileStorage) initDB() {
	s.db.Exec("create table if not exists metadata (name text, reference text, metadata text)")
	s.db.Exec("create index if not exists idx_metadata_name on metadata(name)")
	s.db.Exec("create index if not exists idx_metadata_reference on metadata(reference)")
	s.db.Exec("create index if not exists idx_metadata_name_reference on metadata(name, reference)")
}

func NewFileStorage(path string) (*FileStorage, error) {
	var storage FileStorage

	os.Mkdir(path, os.ModeDir)
	db, err := sql.Open("sqlite3", filepath.Join(path, "metadata.db"))
	if err != nil {
		return nil, err
	}
	storage.db = db
	storage.initDB()

	return &storage, nil
}

func (storage *FileStorage) GetByReference(reference string) (Node, error) {
	if storage == nil {
		return nil, errors.New("storage is nil")
	}

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	rows, err := storage.db.Query("select name, reference, metadata from metadata where reference = ?", reference)
	if err != nil {
		return nil, err
	}

	var resultName *string
	var resultReference *string
	var resultMetadataJson *string

	err = rows.Scan(&resultName, &resultReference, &resultMetadataJson)
	if err != nil {
		return nil, err
	}

	var node FileNode
	node.name = *resultName
	node.reference = *resultReference

	if resultMetadataJson != nil {
		err = json.Unmarshal([]byte(*resultReference), node.metadata)
		if err != nil {
			node.metadata = nil
		}
	}

	return &node, nil
}

func (storage *FileStorage) GetByName(name string) (Node, error) {
	if storage == nil {
		return nil, errors.New("storage is nil")
	}

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	rows, err := storage.db.Query("select name, reference, metadata from metadata where name = ?", name)
	if err != nil {
		return nil, err
	}

	var resultName *string
	var resultReference *string
	var resultMetadataJson *string

	err = rows.Scan(&resultName, &resultReference, &resultMetadataJson)
	if err != nil {
		return nil, err
	}

	var node FileNode
	node.name = *resultName
	node.reference = *resultReference

	if resultMetadataJson != nil {
		err = json.Unmarshal([]byte(*resultReference), node.metadata)
		if err != nil {
			node.metadata = nil
		}
	}

	return &node, nil
}

func (storage *FileStorage) Create(name string) error {
	return nil
}

func (storage *FileStorage) Exists(name string) error {
	return nil
}

func (storage *FileStorage) List(namePrefix string, namePostfix string) ([]Node, error) {
	return []Node{}, nil

}

func (storage *FileStorage) ListReferences(namePrefix string, namePostfix string) ([]string, error) {
	return []string{}, nil
}

func (storage *FileStorage) ListNames(namePrefix string, namePostfix string) ([]string, error) {
	return []string{}, nil
}

func (storage *FileStorage) Open() error {
	return nil
}

func (storage *FileStorage) Close() error {
	return nil
}

type FileNode struct {
	reference string
	name      string
	metadata  interface{}
}

func (node *FileNode) GetReference() string {
	if node == nil {
		panic("node is nil")
	}

	return node.reference
}

func (node *FileNode) GetName() string {
	if node == nil {
		panic("node is nil")
	}

	return node.name
}

func (node *FileNode) GetMetadata() interface{} {
	if node == nil {
		panic("node is nil")
	}

	return node.metadata
}
