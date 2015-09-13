package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/schema"
	"github.com/gonuts/commander"

	"github.com/dichro/cameloff/db"
)

type stats map[string]int

func (s stats) String() string {
	parts := []string{}
	for t, c := range s {
		parts = append(parts, fmt.Sprintf("%s: %d", t, c))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

type status struct {
	location        string
	needs, neededBy []string
}

func (b *status) resolve(ref string) {
	needs := make([]string, 0, len(b.needs)-1)
	for _, n := range b.needs {
		if n != ref {
			needs = append(needs, n)
		}
	}
	b.needs = needs
}

func main() {
	var dbDir, blobDir string

	scan := &commander.Command{
		UsageLine: "scan scans a diskpacked blobstore",
	}
	restart := scan.Flag.Bool("restart", false, "Restart scan from start, ignoring prior progress")
	scan.Run = func(*commander.Command, []string) error {
		scanBlobs(dbDir, blobDir, *restart)
		return nil
	}

	missing := &commander.Command{
		UsageLine: "missing prints unresolved references",
		Run: func(*commander.Command, []string) error {
			return missingBlobs(dbDir, blobDir)
		},
	}

	stats := &commander.Command{
		UsageLine: "stats prints index stats",
		Run: func(*commander.Command, []string) error {
			return statsBlobs(dbDir)
		},
	}

	list := &commander.Command{
		UsageLine: "list lists blobs from the index",
	}
	camliType := list.Flag.String("camliType", "", "Type of blob to list")
	list.Run = func(*commander.Command, []string) error {
		return listBlobs(dbDir, *camliType)
	}

	top := &commander.Command{
		UsageLine: os.Args[0],
		Subcommands: []*commander.Command{
			scan,
			missing,
			stats,
			list,
		},
	}

	// add --db_dir flag to everything
	for _, cmd := range top.Subcommands {
		cmd.Flag.StringVar(&dbDir, "db_dir", "", "FSCK state database directory")
	}

	// add --blob_dir as appropriate
	for _, cmd := range []*commander.Command{scan, missing} {
		cmd.Flag.StringVar(&blobDir, "blob_dir", "", "Camlistore blob directory")
	}

	if err := top.Dispatch(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func listBlobs(dbDir, camliType string) error {
	fsck, err := db.New(dbDir) // read-only?
	if err != nil {
		return err
	}
	for ref := range fsck.List(camliType) {
		fmt.Println(ref)
	}
	return nil
}

func missingBlobs(dbDir, blobDir string) error {
	fsck, err := db.New(dbDir) // read-only?
	if err != nil {
		return err
	}
	defer fsck.Close()
	bs, err := dir.New(blobDir)
	if err != nil {
		return err
	}
	// defer s.Close() - where is this??
	missing := 0
	// TODO(dichro): cache Parents() call results?
	for ref := range fsck.Missing() {
		if body, size, err := bs.Fetch(blob.MustParse(ref)); err == nil {
			log.Printf("missing ref %q found with size %d", ref, size)
			body.Close()
			continue
		}
		fmt.Println(ref)
		missing++
		nodes, err := fsck.Parents(ref)
		if err != nil {
			log.Print(err)
			continue
		}
		printHierarchy(fsck, bs, 1, "", nodes)
	}
	fmt.Println("total", missing)
	return nil
}

func printHierarchy(fsck *db.DB, bs blob.Fetcher, depth int, suffix string, nodes []string) {
	prefix := ""
	for i := 0; i < depth; i++ {
		prefix = prefix + "  "
	}
	for _, node := range nodes {
		nextSuffix := suffix
		ref := blob.MustParse(node)
		camliType := "unknown"
		if body, _, err := bs.Fetch(ref); err != nil {
			camliType = fmt.Sprintf("Fetch(): %s", err)
		} else {
			if s, ok := parseSchema(ref, body); ok {
				fileName := s.FileName()
				switch t := s.Type(); t {
				case "file":
					// this blob is a "file" that just happens to contain a
					// camlistore blob in its contents. This happens because I
					// may have camput my blobs directory once or twice :P
					if len(suffix) > 0 {
						nextSuffix = fmt.Sprintf("%s -> %s", fileName, nextSuffix)
					} else {
						nextSuffix = fileName
					}
					camliType = fmt.Sprintf("%s: %q", t, nextSuffix)
				case "directory":
					nextSuffix = fmt.Sprintf("%s/%s", fileName, suffix)
					camliType = fmt.Sprintf("%s: %q", t, nextSuffix)
				default:
					camliType = t
				}
			}
			body.Close()
		}

		switch next, err := fsck.Parents(node); {
		case err != nil:
			fmt.Printf("%s* %s: %s\n", prefix, node, err)
		case len(next) == 0:
			fmt.Printf("%s- %s (%s)\n", prefix, node, camliType)
		default:
			fmt.Printf("%s+ %s (%s)\n", prefix, node, camliType)
			printHierarchy(fsck, bs, depth+1, nextSuffix, next)
		}
	}
}

func statsBlobs(dbDir string) error {
	fsck, err := db.New(dbDir) // read-only?
	if err != nil {
		return err
	}
	s := fsck.Stats()
	fmt.Println(s)
	if len(s.CamliTypes) == 0 {
		return nil
	}
	fmt.Println("camliTypes:")
	camliTypes := []string{}
	for t := range s.CamliTypes {
		camliTypes = append(camliTypes, t)
	}
	sort.Strings(camliTypes)
	for _, t := range camliTypes {
		fmt.Printf("\t%q: %d\n", t, s.CamliTypes[t])
	}
	return nil
}

func scanBlobs(dbDir, blobDir string, restart bool) {
	statsCh := time.Tick(10 * time.Second)

	fsck, err := db.New(dbDir)
	if err != nil {
		log.Fatal(err)
	}

	last := fsck.Last()
	if last != "" {
		if restart {
			fmt.Println("overwriting blob scan resume marker at", last)
			last = ""
		} else {
			fmt.Println("resuming blob scan at", last)
		}
	}

	blobCh := streamBlobs(blobDir, last)

	stats := make(stats)
	for {
		select {
		case <-statsCh:
			fmt.Println(time.Now(), stats)
		case b, ok := <-blobCh:
			if !ok {
				return
			}
			if !b.ValidContents() {
				stats["corrupt"]++
				continue
			}
			ref := b.Ref()
			body := b.Open()
			s, ok := parseSchema(ref, body)
			body.Close()
			if !ok {
				stats["data"]++
				if err := fsck.Place(ref.String(), b.Token, "", nil); err != nil {
					log.Fatal(err)
				}
				continue
			}
			t := s.Type()
			stats[t]++
			var needs []string
			switch t {
			case "static-set":
				for _, r := range s.StaticSetMembers() {
					needs = append(needs, r.String())
				}
			case "bytes":
				fallthrough
			case "file":
				for i, bp := range s.ByteParts() {
					ok := false
					if r := bp.BlobRef; r.Valid() {
						needs = append(needs, r.String())
						ok = true
					}
					if r := bp.BytesRef; r.Valid() {
						needs = append(needs, r.String())
						ok = true
					}
					if !ok {
						log.Printf("%s (%s): no valid ref in part %d", ref, t, i)
					}
				}
			case "directory":
				switch r, ok := s.DirectoryEntries(); {
				case !ok:
					log.Printf("%s (%s): bad entries", ref, t)
				case !r.Valid():
					log.Printf("%s (%s): invalid entries", ref, t)
				default:
					needs = append(needs, r.String())
				}
			}
			if err := fsck.Place(ref.String(), b.Token, t, needs); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func parseSchema(ref blob.Ref, body io.Reader) (*schema.Blob, bool) {
	sn := index.NewBlobSniffer(ref)
	io.Copy(sn, body)
	sn.Parse()
	return sn.SchemaBlob()
}

func streamBlobs(path, resume string) <-chan blobserver.BlobAndToken {
	s, err := dir.New(path)
	if err != nil {
		log.Fatal(err)
	}
	bs, ok := s.(blobserver.BlobStreamer)
	if !ok {
		log.Fatalf("%v is not a BlobStreamer", s)
	}

	ch := make(chan blobserver.BlobAndToken, 10)
	go func() {
		ctx := context.New()
		if err := bs.StreamBlobs(ctx, ch, resume); err != nil {
			log.Fatal(err)
		}
	}()
	return ch
}
