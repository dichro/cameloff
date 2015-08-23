package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"github.com/gonuts/commander"
)

type Flag struct {
	Path string
	BS   blobserver.Storage
}

func (f *Flag) Set(path string) error {
	f.Path = path
	s, err := dir.New(path)
	_, ok := s.(blobserver.BlobStreamer)
	switch {
	case err != nil:
		return err
	case !ok:
		return errors.New("not a diskpacked repository")
	default:
		f.BS = s
	}
	return nil
}

func (f *Flag) String() string {
	return f.Path
}

func (f *Flag) Get() interface{} {
	return f.Path
}

func main() {
	bs := Flag{}

	cat := &commander.Command{
		UsageLine: "cat prints blob contents",
		Run: func(cmd *commander.Command, args []string) error {
			if bs.BS == nil {
				return errors.New("require --blob_dir")
			}
			for _, ref := range args {
				br, ok := blob.Parse(ref)
				if !ok {
					fmt.Fprintf(os.Stderr, "couldn't parse ref\n")
					continue
				}
				blob, _, err := bs.BS.Fetch(br)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: %s\n", ref, err)
					continue
				}
				io.Copy(os.Stdout, blob)
				blob.Close()
			}
			return nil
		},
	}

	top := &commander.Command{
		UsageLine: os.Args[0],
		Subcommands: []*commander.Command{
			cat,
		},
	}

	for _, cmd := range top.Subcommands {
		cmd.Flag.Var(&bs, "blob_dir", "Camlistore blob directory")
	}

	if err := top.Dispatch(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}
