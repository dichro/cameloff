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
	"github.com/dichro/cameloff/db"
)

var (
	blobDir = flag.String("blob_dir", "", "Camlistore blob directory")
	dbDir   = flag.String("db_dir", "", "FSCK state database directory")
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

func (bs blobs) needs(by string, needed []string) {
	byStatus := bs[by]
	for _, n := range needed {
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

	fsck, err := db.New(*dbDir)
	if err != nil {
		log.Fatal(err)
	}

	last := fsck.Last()
	if last != "" {
		fmt.Println("resuming blob scan at", last)
	}

	blobCh := streamBlobs(*blobDir, last)

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
				if err := fsck.Place(ref, b.Token, nil); err != nil {
					log.Fatal(err)
				}
				continue
			}
			t := s.Type()
			stats[t]++
			needs := []string{}
			switch t {
			case "static-set":
				for _, r := range s.StaticSetMembers() {
					needs = append(needs, r.String())
				}
			case "file":
				for _, bp := range s.ByteParts() {
					if r := bp.BlobRef; r.Valid() {
						needs = append(needs, r.String())
					}
					if r := bp.BytesRef; r.Valid() {
						needs = append(needs, r.String())
					}
				}
			}
			if err := fsck.Place(ref, b.Token, needs); err != nil {
				log.Fatal(err)
			}
			if len(needs) > 0 {
				blobs.needs(ref, needs)
			}
		}
	}
}

func streamBlobs(path, resume string) <-chan blobserver.BlobAndToken {
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
		if err := bs.StreamBlobs(ctx, ch, resume); err != nil {
			log.Fatal(err)
		}
	}()
	return ch
}
