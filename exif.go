package main

import (
	"flag"
	"fmt"
	"log"

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

	files := fsck.NewFiles(bs)
	go files.ReadRefs(fdb.ListMIME(*mimeType))
	go files.LogErrors()
	for r := range files.Readers {
		ex, err := exif.Decode(r)
		if err != nil {
			log.Print(err)
			continue
		}
		fmt.Println(ex)
	}
}
