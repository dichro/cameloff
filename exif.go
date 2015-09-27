package main

import (
	"flag"
	"fmt"
	"log"

	"camlistore.org/pkg/blobserver/dir"
	"github.com/rwcarlsen/goexif/exif"

	"github.com/dichro/cameloff/db"
)

func main() {
	dbDir := flag.String("db_dir", "", "FSCK state database directory")
	blobDir := flag.String("blob_dir", "", "Camlistore blob directory")
	mimeType := flag.String("mime_type", "image/jpeg", "MIME type of files to scan")
	flag.Parse()

	fsck, err := db.New(*dbDir)
	if err != nil {
		log.Fatal(err)
	}
	bs, err := dir.New(*blobDir)
	if err != nil {
		log.Fatal(err)
	}

	files := db.NewFiles(bs)
	go files.ReadRefs(fsck.ListMIME(*mimeType))
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
