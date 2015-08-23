package db

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	found   = "found"
	missing = "missing"
)

// Place notes the presence of a blob at a particular location with
// known dependencies.
func (d *DB) Place(ref, location string, dependencies []string) (err error) {
	b := new(leveldb.Batch)
	// TODO(dichro): duplicates are interesting, but pretty rare,
	// so probably not worth tracking?
	b.Put(pack(found, ref), pack(location))
	b.Put(lastKey, []byte(location))
	for _, dep := range dependencies {
		b.Put(pack("parent", dep, ref), nil)
		// TODO(dichro): should these always be looked up
		// inline? Maybe a post-scan would be faster for bulk
		// insert?
		if ok, _ := d.db.Has(pack(found, dep), nil); !ok {
			b.Put(pack(missing, dep, ref), nil)
		}
	}
	it := d.db.NewIterator(&util.Range{
		Start: pack(missing, ref),
		Limit: pack(missing, ref, "z"),
	}, nil)
	defer it.Release()
	for it.Next() {
		b.Delete(it.Key())
	}
	if err := it.Error(); err != nil {
		fmt.Println(err)
	}
	err = d.db.Write(b, nil)
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

// Missing returns the currently unknown blobs.
func (d *DB) Missing() <-chan string {
	ch := make(chan string)
	go d.missing(ch)
	return ch
}

func (d *DB) missing(ch chan<- string) {
	it := d.db.NewIterator(&util.Range{
		Start: pack(missing),
	}, nil)
	defer it.Release()
	for it.Next() {
		parts := unpack(it.Key())
		ch <- parts[1]
	}
	close(ch)
}

func pack(prefix string, fields ...string) []byte {
	b := bytes.NewBufferString(prefix)
	for _, f := range fields {
		b.WriteByte('|')
		b.WriteString(f)
	}
	return b.Bytes()
}

func unpack(bts []byte) []string {
	return strings.Split(string(bts), "|")
}
