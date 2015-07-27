package main

import (
	"flag"
	"log"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/index"
)

var (
	blobDir = flag.String("blob_dir", "", "Camlistore blob directory")
)

func main() {
	flag.Parse()

	if len(*blobDir) == 0 {
		flag.Usage()
		return
	}
	s, err := dir.New(*blobDir)
	if err != nil {
		log.Fatal(err)
	}

	in := index.NewMemoryIndex()
	in.InitBlobSource(s)

	for _, arg := range flag.Args() {
		br, ok := blob.Parse(arg)
		if !ok {
			log.Fatal("unparseable ", arg)
		}
		b, err := blob.FromFetcher(s, br)
		if err != nil {
			log.Fatal(err)
		}
		r := b.Open()
		_, err = in.ReceiveBlob(br, r)
		if err != nil {
			log.Fatal(err)
		}
		r.Close()
	}
}
