package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/index"
	"github.com/dichro/cameloff/db"
	"github.com/gonuts/commander"
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
	var dbDir string

	scan := &commander.Command{
		UsageLine: "scan scans a diskpacked blobstore",
	}
	blobDir := scan.Flag.String("blob_dir", "", "Camlistore blob directory")
	restart := scan.Flag.Bool("restart", false, "Restart scan from start, ignoring prior progress")
	scan.Run = func(*commander.Command, []string) error {
		scanBlobs(dbDir, *blobDir, *restart)
		return nil
	}

	missing := &commander.Command{
		UsageLine: "missing prints unresolved references",
		Run: func(*commander.Command, []string) error {
			return missingBlobs(dbDir)
		},
	}

	stats := &commander.Command{
		UsageLine: "stats prints index stats",
		Run: func(*commander.Command, []string) error {
			return statsBlobs(dbDir)
		},
	}

	top := &commander.Command{
		UsageLine: os.Args[0],
		Subcommands: []*commander.Command{
			scan,
			missing,
			stats,
		},
	}

	// add --db_dir flag to everything
	for _, cmd := range top.Subcommands {
		cmd.Flag.StringVar(&dbDir, "db_dir", "", "FSCK state database directory")
	}

	if err := top.Dispatch(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func missingBlobs(dbDir string) error {
	fsck, err := db.New(dbDir) // read-only?
	if err != nil {
		return err
	}
	roots := map[string]int{}
	missing := 0
	// TODO(dichro): cache Parents() call results?
	for ref := range fsck.Missing() {
		missing++
		nodes, err := fsck.Parents(ref)
		if err != nil {
			log.Print(err)
			continue
		}
		for len(nodes) > 0 {
			n := nodes[0]
			nodes = nodes[1:]
			switch p, err := fsck.Parents(n); {
			case err != nil:
				log.Print(err)
			case len(p) == 0:
				roots[n] += 1
			default:
				// TODO(dichro): loop detection
				nodes = append(nodes, p...)
			}
		}
	}
	fmt.Println("total", missing)
	refs := make([]string, 0, len(roots))
	for r := range roots {
		refs = append(refs, r)
	}
	sort.Strings(refs)
	for _, r := range refs {
		fmt.Println(r, roots[r])
	}
	return nil
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
		fmt.Printf("\t%s: %d\n", t, s.CamliTypes[t])
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
			//
			ref := b.Ref().String()

			//
			body := b.Open()
			sn := index.NewBlobSniffer(b.Ref())
			io.Copy(sn, body)
			body.Close()
			sn.Parse()
			s, ok := sn.SchemaBlob()
			if !ok {
				stats["data"]++
				if err := fsck.Place(ref, b.Token, "data", nil); err != nil {
					log.Fatal(err)
				}
				continue
			}
			t := s.Type()
			stats[t]++
			needs := []string{}
			switch t {
			case "static-set":
				for _, r := range s.StaticSetMembers() {
					needs = append(needs, r.String())
				}
			case "file":
				for _, bp := range s.ByteParts() {
					if r := bp.BlobRef; r.Valid() {
						needs = append(needs, r.String())
					}
					if r := bp.BytesRef; r.Valid() {
						needs = append(needs, r.String())
					}
				}
			}
			if err := fsck.Place(ref, b.Token, t, needs); err != nil {
				log.Fatal(err)
			}
		}
	}
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
