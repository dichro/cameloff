package fsck

import (
	"io"
	"log"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/schema"
)

// File is an opened file from the repo.
type File struct {
	io.Reader
	Ref, Filename string
}

// Files provides a stream of open file readers from the repo.
type Files struct {
	// Blob source
	Fetcher blob.Fetcher
	// File readers
	Readers chan File
	// Channels reporting various errors
	Missing, Invalid, Unreadable chan string
}

func NewFiles(fetcher blob.Fetcher) *Files {
	return &Files{
		fetcher,
		make(chan File),
		make(chan string),
		make(chan string),
		make(chan string),
	}
}

// ReadRefs opens all files corresponding to the refs supplied on the
// provided channel.
func (f Files) ReadRefs(refs <-chan string) {
	for ref := range refs {
		ref := ref
		br := blob.MustParse(ref)
		body, _, err := f.Fetcher.Fetch(br)
		if err != nil {
			f.Missing <- ref
			continue
		}
		s, ok := parseSchema(br, body)
		body.Close()
		if !ok {
			f.Invalid <- ref
			continue
		}
		file, err := s.NewFileReader(f.Fetcher)
		if err != nil {
			f.Unreadable <- ref
			continue
		}
		f.Readers <- File{Reader: file, Ref: ref, Filename: s.FileName()}
	}
}

func parseSchema(ref blob.Ref, body io.Reader) (*schema.Blob, bool) {
	sn := index.NewBlobSniffer(ref)
	io.Copy(sn, body)
	sn.Parse()
	return sn.SchemaBlob()
}

// LogErrors is a utility routine for dumping all encountered errors
// to logs.
func (f Files) LogErrors() {
	for {
		select {
		case ref, ok := <-f.Missing:
			if !ok {
				return
			}
			log.Printf("%s: previously indexed; now missing", ref)
		case ref, ok := <-f.Invalid:
			if !ok {
				return
			}
			log.Printf("%s: previously schema blob; now unparseable", ref)
		case ref, ok := <-f.Unreadable:
			if !ok {
				return
			}
			log.Printf("%s: unreadable", ref)
		}
	}
}
