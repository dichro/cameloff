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

const (
	found     = "found"
	missing   = "missing"
	parent    = "parent"
	last      = "last"
	camliType = "type"

	start = "\x00"
	limit = "\xff"
)

// Place notes the presence of a blob at a particular location.
func (d *DB) Place(ref, location, ct string, dependencies []string) (err error) {
	b := new(leveldb.Batch)
	// TODO(dichro): duplicates are interesting, but pretty rare,
	// so probably not worth tracking?
	b.Put(pack(found, ref), pack(location))
	b.Put(pack(last), pack(location))
	b.Put(pack(camliType, ct, ref), nil)
	for _, dep := range dependencies {
		b.Put(pack(parent, dep, ref), nil)
		// TODO(dichro): should these always be looked up
		// inline? Maybe a post-scan would be faster for bulk
		// insert?
		if ok, _ := d.db.Has(pack(found, dep), nil); !ok {
			b.Put(pack(missing, dep, ref), nil)
		}
	}
	it := d.db.NewIterator(&util.Range{
		Start: pack(missing, ref, start),
		Limit: pack(missing, ref, limit),
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
	if data, err := d.db.Get(pack(last), nil); err == nil {
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
		Start: pack(missing, start),
		Limit: pack(missing, limit),
	}, nil)
	defer it.Release()
	for it.Next() {
		parts := unpack(it.Key())
		ch <- parts[1]
	}
	close(ch)
}

type Stats struct {
	Blobs, Links, Missing, Unknown uint64
	CamliTypes                     map[string]int64
}

func (s Stats) String() string {
	return fmt.Sprintf("%d blobs, %d links, %d missing; %d unknown index entries",
		s.Blobs, s.Links, s.Missing, s.Unknown)
}

func (d *DB) Stats() (s Stats) {
	s.CamliTypes = make(map[string]int64)
	it := d.db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		parts := unpack(it.Key())
		switch parts[0] {
		case last:
		case found:
			s.Blobs++
		case parent:
			s.Links++
		case missing:
			s.Missing++
		case camliType:
			s.CamliTypes[parts[1]]++
		default:
			s.Unknown++
		}
	}
	return
}

func (d *DB) Parents(ref string) (parents []string, err error) {
	it := d.db.NewIterator(&util.Range{
		Start: pack(parent, ref, start),
		Limit: pack(parent, ref, limit),
	}, nil)
	defer it.Release()
	for it.Next() {
		parts := unpack(it.Key())
		parents = append(parents, parts[2])
	}
	err = it.Error()
	return
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
