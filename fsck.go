package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	"camlistore.org/pkg/blob"
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

type status struct {
	location        string
	needs, neededBy []string
}

func (b *status) resolve(ref string) {
	needs := make([]string, 0, len(b.needs)-1)
	for _, n := range b.needs {
		if n != ref {
			needs = append(needs, n)
		}
	}
	b.needs = needs
}

type blobs map[string]*status

func (bs blobs) place(ref, location string) (b *status, dup bool) {
	b, dup = bs[ref]
	if !dup {
		// first mention of this blob ever
		b = &status{location: location}
		bs[ref] = b
		return
	}
	if len(b.location) == 0 {
		// first concrete instance of this blob
		dup = false
	}
	for _, needer := range b.neededBy {
		bs[needer].resolve(ref)
	}
	b.neededBy = nil
	return
}

func (bs blobs) needs(by string, needed []blob.Ref) {
	if len(needed) == 0 {
		return
	}
	byStatus := bs[by]
	for _, n := range needed {
		n := n.String()
		if b, ok := bs[n]; ok {
			if len(b.location) == 0 {
				b.neededBy = append(b.neededBy, by)
			}
		} else {
			bs[n] = &status{neededBy: []string{by}}
			byStatus.needs = append(byStatus.needs, n)
		}
	}
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
			fmt.Println(time.Now(), stats)
			var ok, pending, missing int
			for _, b := range blobs {
				switch {
				case len(b.location) == 0:
					missing++
				case len(b.needs) != 0:
					pending++
				default:
					ok++
				}
			}
			fmt.Println(time.Now(), "ok", ok, "pending", pending, "missing", missing)
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
				blobs.needs(ref, s.StaticSetMembers())
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
