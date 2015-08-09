package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/context"
)

var (
	blobDir = flag.String("blob_dir", "", "Camlistore blob directory")
)

func main() {
	flag.Parse()

	statsCh := time.Tick(10 * time.Second)
	blobCh := streamBlobs(*blobDir)

	var (
		blobs int
	)
	for {
		select {
		case <-statsCh:
			fmt.Println(blobs, "blobs")
		case _, ok := <-blobCh:
			if !ok {
				return
			}
			blobs++
		}
	}
}

func streamBlobs(path string) <-chan blobserver.BlobAndToken {
	s, err := dir.New(path)
	if err != nil {
		log.Fatal(err)
	}
	bs, ok := s.(blobserver.BlobStreamer)
	if !ok {
		log.Fatalf("%v is not a BlobStreamer", s)
	}

	ch := make(chan blobserver.BlobAndToken, 10)
	go func() {
		defer close(ch)
		ctx := context.New()
		if err := bs.StreamBlobs(ctx, ch, ""); err != nil {
			log.Fatal(err)
		}
	}()
	return ch
}
