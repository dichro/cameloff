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

type stats map[string]int

func (s stats) String() string {
	parts := []string{}
	for t, c := range s {
		parts = append(parts, fmt.Sprintf("%s: %d", t, c))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

type blob struct {
	location        string
	needs, neededBy []string
}

type blobs map[string]*blob

func (bs blobs) place(ref, location string) (b *blob, dup bool) {
	b, dup = bs[ref]
	if !dup {
		// first mention of this blob ever
		b = &blob{location: location}
		bs[ref] = b
		return
	}
	if len(b.location) == 0 {
		// first concrete instance of this blob
		dup = false
	}
	return
}

func main() {
	flag.Parse()

	statsCh := time.Tick(10 * time.Second)
	blobCh := streamBlobs(*blobDir)

	stats := make(stats)
	blobs := make(blobs)
	for {
		select {
		case <-statsCh:
			fmt.Println(stats)
		case b, ok := <-blobCh:
			if !ok {
				return
			}
			if !b.ValidContents() {
				stats["corrupt"]++
				continue
			}
			//
			ref := b.Ref().String()
			_, dup := blobs.place(ref, b.Token)
			if dup {
				stats["dup"]++
			}

			//
			body := b.Open()
			sn := index.NewBlobSniffer(b.Ref())
			io.Copy(sn, body)
			body.Close()
			sn.Parse()
			s, ok := sn.SchemaBlob()
			if !ok {
				stats["data"]++
				continue
			}
			t := s.Type()
			stats[t]++
			switch t {
			case "static-set":

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
