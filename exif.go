package main

import (
	"flag"
	"log"
	"time"

	"camlistore.org/pkg/blobserver/dir"
	"github.com/rwcarlsen/goexif/exif"

	"github.com/dichro/cameloff/db"
	"github.com/dichro/cameloff/fsck"
)

func main() {
	dbDir := flag.String("db_dir", "", "FSCK state database directory")
	blobDir := flag.String("blob_dir", "", "Camlistore blob directory")
	mimeType := flag.String("mime_type", "image/jpeg", "MIME type of files to scan")
	flag.Parse()

	fdb, err := db.New(*dbDir)
	if err != nil {
		log.Fatal(err)
	}
	bs, err := dir.New(*blobDir)
	if err != nil {
		log.Fatal(err)
	}

	stats := fsck.NewStats()
	defer stats.LogTopNEvery(10, 10*time.Second).Stop()
	defer log.Print(stats)

	files := fsck.NewFiles(bs)
	go files.ReadRefs(fdb.ListMIME(*mimeType))
	go files.LogErrors()

	workers := fsck.Parallel{Workers: 32}
	workers.Go(func() {
		for r := range files.Readers {
			ex, err := exif.Decode(r)
			if err != nil {
				stats.Add("error")
				continue
			}
			if tag, err := ex.Get(exif.Model); err == nil {
				stats.Add(tag.String())
			} else {
				stats.Add("missing")
			}
		}
	})
	workers.Wait()
}
