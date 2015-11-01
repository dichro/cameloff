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
	go func() {
		files.ReadRefs(fdb.ListMIME(*mimeType))
		files.Close()
	}()
	go files.LogErrors()

	workers.Go(func() {
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
				id := "unknown"
				if tag, err := ex.Get(exif.ImageUniqueID); err == nil {
					id = tag.String()
					stats.Add("unique-id-exif")
				} else if thumb, err := ex.JpegThumbnail(); err == nil {
					hash := sha1.Sum(thumb)
					id = hex.EncodeToString(hash[:20])
					stats.Add("unique-id-thumb")
				} else if r.PartsSize() < 1e7 {
					if _, err := r.Seek(0, 0); err == nil {
						hash := sha1.New()
						io.Copy(hash, r)
						id = hex.EncodeToString(hash.Sum(nil))
						stats.Add("unique-id-sha1")
					} else {
						id = "read-error"
						stats.Add("unique-id-sha1-error")
					}
				} else {
					stats.Add("unique-id-too-big")
				}
				fmt.Printf("%s %s %q %q\n", r.BlobRef(), id, r.FileName(), tag)
			}
		}
	})
	workers.Wait()
}
