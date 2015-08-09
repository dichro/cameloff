package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/index"
)

var (
	blobDir = flag.String("blob_dir", "", "Camlistore blob directory")
)

type stats struct {
	corrupt, data int
	types         map[string]int
}

func (s stats) String() string {
	parts := []string{fmt.Sprintf("corrupt: %d, data %d", s.corrupt, s.data)}
	for t, c := range s.types {
		parts = append(parts, fmt.Sprintf("%s: %d", t, c))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func main() {
	flag.Parse()

	statsCh := time.Tick(10 * time.Second)
	blobCh := streamBlobs(*blobDir)

	stats := stats{types: make(map[string]int)}
	for {
		select {
		case <-statsCh:
			fmt.Println(stats)
		case b, ok := <-blobCh:
			if !ok {
				return
			}
			if !b.ValidContents() {
				stats.corrupt++
			}
			body := b.Open()
			sn := index.NewBlobSniffer(b.Ref())
			io.Copy(sn, body)
			body.Close()
			sn.Parse()
			s, ok := sn.SchemaBlob()
			if !ok {
				stats.data++
			} else {
				stats.types[s.Type()]++
			}
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
