package main

import (
	"flag"
	"fmt"
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

	for i, arg := range flag.Args() {
		fmt.Println(i, "index entries", count(in))
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
	fmt.Println("end index entries", count(in))
}

func count(in *index.Index) (entries int) {
	for it := in.Storage().Find("", ""); it.Next(); entries++ {
	}
	return
}
