package db

import (
	"bytes"
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

type DB struct {
	db *leveldb.DB
}

func New(path string) (*DB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

var (
	lastKey = []byte("last")
)

// Place notes the presence of a blob at a particular location.
func (d *DB) Place(ref, location string) (err error) {
	err = d.db.Put(bytes.NewBufferString(fmt.Sprintf("place|%s|%s", ref, location)).Bytes(), nil, nil)
	if err == nil {
		err = d.db.Put(lastKey, []byte(location), nil)
	}
	return
}

// Last returns the last location successfully Placed.
func (d *DB) Last() string {
	if data, err := d.db.Get(lastKey, nil); err == nil {
		return string(data)
	} else {
		log.Print(err)
	}
	return ""
}
