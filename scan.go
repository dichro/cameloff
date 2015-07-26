package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/sorted/leveldb"
)

var (
	blobDir  = flag.String("blob_dir", "", "Camlistore blob directory")
	indexDir = flag.String("index_dir", "", "New leveldb index directory")
)

type FetcherEnumerator struct {
	blobserver.FetcherEnumerator
	start time.Time

	mu               sync.Mutex
	c1, c2           map[string]*blob.Blob
	hit1, hit2, miss int
	blobs, bytes     int64
	updated          time.Time
}

const cacheSize = 4000

func (f *FetcherEnumerator) Add(b *blob.Blob) {
	f.c1[b.Ref().Digest()] = b
	if len(f.c1) >= cacheSize {
		f.c2 = f.c1
		f.c1 = make(map[string]*blob.Blob, cacheSize)
	}
}

func (f *FetcherEnumerator) CacheFetch(ref blob.Ref) *blob.Blob {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := ref.Digest()
	if b, ok := f.c1[d]; ok {
		f.hit1++
		return b
	}
	if b, ok := f.c2[d]; ok {
		f.hit2++
		return b
	}
	f.miss++
	return nil
}

func (f *FetcherEnumerator) Fetch(ref blob.Ref) (io.ReadCloser, uint32, error) {
	if b := f.CacheFetch(ref); b != nil {
		return b.Open(), b.Size(), nil
	}
	return f.FetcherEnumerator.Fetch(ref)
}

func (f *FetcherEnumerator) Index(ch chan blobserver.BlobAndToken, dst *index.Index) {
	for b := range ch {
		valid := b.ValidContents()

		f.mu.Lock()
		if valid {
			b := b
			f.Add(b.Blob)
		}
		f.blobs++
		f.bytes += int64(b.Size())
		blobs, bytes := f.blobs, f.bytes
		hit1, hit2, miss := f.hit1, f.hit2, f.miss

		now := time.Now()
		updated := now.Sub(f.updated) > 10*time.Second
		if updated {
			f.updated = now
		}
		f.mu.Unlock()

		if updated {
			delta := now.Sub(f.start).Seconds()
			fmt.Printf("%s: %8d (%4.0f/sec) blobs %12d (%4.0fk/sec) bytes %8d/%-8d hit %8d miss\n",
				now, blobs, float64(blobs)/delta, bytes, float64(bytes)/(delta*1000), hit1, hit2, miss)
		}
		start := time.Now()
		r := b.Open()
		_, err := dst.ReceiveBlob(b.Ref(), r)
		if err != nil {
			log.Print(err)
		}
		r.Close()
		if elapsed := time.Now().Sub(start); elapsed > time.Second {
			fmt.Printf("elapsed %s to index sha1-%s at '%s'\n",
				elapsed, b.Ref().Digest(), b.Token)
			// pull some data out of the index to
			// describe blob? print continuation
			// token for easier restart?
		}
	}
}

func main() {
	flag.Parse()

	if len(*blobDir)*len(*indexDir) == 0 {
		flag.Usage()
		return
	}
	s, err := dir.New(*blobDir)
	if err != nil {
		log.Fatal(err)
	}
	src, ok := s.(blobserver.BlobStreamer)
	if !ok {
		log.Fatalf("%v is not a BlobStreamer", s)
	}

	db, err := leveldb.NewStorage(*indexDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	dst, err := index.New(db)
	if err != nil {
		log.Fatal(err)
	}

	fe := FetcherEnumerator{
		FetcherEnumerator: s,
		c1:                make(map[string]*blob.Blob, cacheSize),
		start:             time.Now(),
	}
	dst.InitBlobSource(&fe)

	ch := make(chan blobserver.BlobAndToken)
	go fe.Index(ch, dst)
	go fe.Index(ch, dst)
	ctx := context.New()
	if err := src.StreamBlobs(ctx, ch, ""); err != nil {
		log.Fatal(err)
	}
}
