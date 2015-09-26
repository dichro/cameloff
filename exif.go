package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/dichro/cameloff/db"
)

func main() {
	dbDir := flag.String("db_dir", "", "FSCK state database directory")
	mimeType := flag.String("mime_type", "image/jpeg", "MIME type of files to scan")
	flag.Parse()

	fsck, err := db.New(*dbDir)
	if err != nil {
		log.Fatal(err)
	}
	files := 0
	for f := range fsck.ListMIME(*mimeType) {
		fmt.Println(f)
		files++
	}
	fmt.Println("saw", files, "files")
}
