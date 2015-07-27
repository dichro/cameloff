package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"sync"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/index"
)

var (
	blobDir = flag.String("blob_dir", "", "Camlistore blob directory")
)

type fetcher struct {
	blobserver.FetcherEnumerator

	mu      sync.Mutex
	fetches int
}

func (f *fetcher) Fetch(ref blob.Ref) (io.ReadCloser, uint32, error) {
	f.mu.Lock()
	f.fetches++
	f.mu.Unlock()
	return f.FetcherEnumerator.Fetch(ref)
}

func main() {
	flag.Parse()

	if len(*blobDir) == 0 {
		flag.Usage()
		return
	}
	s, err := dir.New(*blobDir)
	if err != nil {
		log.Fatal(err)
	}

	f := &fetcher{FetcherEnumerator: s}
	in := index.NewMemoryIndex()
	in.InitBlobSource(f)

	for i, arg := range flag.Args() {
		fmt.Println(i, "index entries", count(in), "fetches", f.fetches)
		br, ok := blob.Parse(arg)
		if !ok {
			log.Fatal("unparseable ", arg)
		}
		b, err := blob.FromFetcher(s, br)
		if err != nil {
			log.Fatal(err)
		}
		r := b.Open()
		_, err = in.ReceiveBlob(br, r)
		if err != nil {
			log.Fatal(err)
		}
		r.Close()
	}
	fmt.Println("end index entries", count(in), "fetches", f.fetches)
}

func count(in *index.Index) (entries int) {
	for it := in.Storage().Find("", ""); it.Next(); entries++ {
	}
	return
}
