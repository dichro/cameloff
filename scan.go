package main

import (
	"flag"
	"fmt"
	"log"
	"sync"

	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/context"
)

var (
	blobDir = flag.String("blob_dir", "", "Camlistore blob directory")
)

func main() {
	flag.Parse()

	s, err := dir.New(*blobDir)
	if err != nil {
		log.Fatal(err)
	}
	bs, ok := s.(blobserver.BlobStreamer)
	if !ok {
		log.Fatalf("%v is not a BlobStreamer", s)
	}

	ch := make(chan blobserver.BlobAndToken, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var blobs, bytes int64
		var token string

		for b := range ch {
			token = b.Token
			blobs++
			bytes += int64(b.Size())
			if blobs%1000 == 0 {
				fmt.Printf("%6d blobs %10d bytes\n", blobs, bytes)
			}
		}
		fmt.Println("last token", token)
	}()
	ctx := context.New()
	if err := bs.StreamBlobs(ctx, ch, ""); err != nil {
		wg.Wait()
		log.Fatal(err)
	}
}
