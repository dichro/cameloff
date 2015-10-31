package main

import (
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
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
	print := flag.Bool("print", false, "Print ref and camera model")
	workers := fsck.Parallel{Workers: 32}
	flag.Var(workers, "workers", "parallel worker goroutines")
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

	workers.Go(func() {
		log.Print("worker start")
		for r := range files.Readers {
			ex, err := exif.Decode(r)
			if err != nil {
				stats.Add("error")
				continue
			}
			tag, err := ex.Get(exif.Model)
			if err != nil {
				stats.Add("missing")
				continue
			}
			stats.Add(tag.String())
			if *print {
				sum := []byte("read-error")
				if _, err := r.Seek(0, 0); err == nil {
					hash := sha1.New()
					io.Copy(hash, r)
					sum = hash.Sum(nil)
				}
				fmt.Printf("%s %s %q %q\n", r.Ref, hex.EncodeToString(sum), r.Filename, tag)
			}
		}
		log.Print("worker end")
	})
	workers.Wait()
}
