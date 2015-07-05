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

	mu               sync.Mutex
	c1, c2           map[string]*blob.Blob
	hit1, hit2, miss int
}

const cacheSize = 4000

func (f *FetcherEnumerator) Add(b *blob.Blob) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.c1[b.Ref().Digest()] = b
	if len(f.c1) >= cacheSize {
		f.c2 = f.c1
		f.c1 = make(map[string]*blob.Blob, cacheSize)
	}
}

func (f *FetcherEnumerator) Fetch(ref blob.Ref) (io.ReadCloser, uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := ref.Digest()
	if b, ok := f.c1[d]; ok {
		f.hit1++
		return b.Open(), b.Size(), nil
	}
	if b, ok := f.c2[d]; ok {
		f.hit2++
		return b.Open(), b.Size(), nil
	}
	f.miss++
	return f.FetcherEnumerator.Fetch(ref)
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
	}
	dst.InitBlobSource(&fe)

	ch := make(chan blobserver.BlobAndToken)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var blobs, bytes int64
		var token string

		t := time.Now()
		for b := range ch {
			token = b.Token
			if b.ValidContents() {
				b := b
				fe.Add(b.Blob)
			}
			blobs++
			bytes += int64(b.Size())
			if blobs%1000 == 0 {
				now := time.Now()
				delta := now.Sub(t).Seconds()
				t = now
				fe.mu.Lock()
				fmt.Printf("%6d (%4.0f/sec) blobs %10d bytes %6d/%-6d hit %6d miss\n",
					blobs, 1000.0/delta, bytes, fe.hit1, fe.hit2, fe.miss)
				fe.mu.Unlock()
			}
			r := b.Open()
			_, err := dst.ReceiveBlob(b.Ref(), r)
			if err != nil {
				log.Print(err)
			}
			r.Close()
		}
		fmt.Println("last token", token)
	}()
	ctx := context.New()
	if err := src.StreamBlobs(ctx, ch, ""); err != nil {
		wg.Wait()
		log.Fatal(err)
	}
}
