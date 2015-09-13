// reindex performs offline reindexing of a diskpacked blobstore via
// BlobStreamer.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/dir"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/sorted/leveldb"

	humanize "github.com/dustin/go-humanize"
)

var (
	blobDir     = flag.String("blob_dir", "", "Camlistore blob directory")
	indexDir    = flag.String("index_dir", "", "New leveldb index directory")
	cacheSizeMB = flag.Int("cache_size_mb", 1024, "Blob cache size in MB")
	parallel    = flag.Int("parallel", 1, "Parallel blobstore walkers")
	streamStart = flag.String("stream_start", "", "Start position for blobstreamer")
)

type stats struct {
	hit, miss    int
	blobs, bytes uint64
}

// FetcherEnumerator does bad caching. Each Index goroutine should
// maintain its own cache, since its likeliest to have the blobs that
// will (ultimately) be used by the honking great big file that it's
// indexing. Having a shared cache means that the only usefully cached
// blobs for that operation will have been evicted by the other
// threads merrily running ahead before the file gets processed to
// that point.
type FetcherEnumerator struct {
	blobserver.FetcherEnumerator
	start time.Time

	mu     sync.Mutex
	cached int64
	c1, c2 map[string]*blob.Blob
	stats  stats
}

func (f *FetcherEnumerator) Add(b *blob.Blob) {
	if *cacheSizeMB <= 0 {
		return
	}
	f.cached += int64(b.Size())
	if f.cached >= int64(*cacheSizeMB)*512*1024 {
		f.cached = 0
		f.c2 = f.c1
		f.c1 = make(map[string]*blob.Blob)
	}
	f.c1[b.Ref().Digest()] = b
}

func (f *FetcherEnumerator) CacheFetch(ref blob.Ref) *blob.Blob {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := ref.Digest()
	if b, ok := f.c1[d]; ok {
		f.stats.hit++
		return b
	}
	if b, ok := f.c2[d]; ok {
		f.stats.hit++
		return b
	}
	f.stats.miss++
	return nil
}

func (f *FetcherEnumerator) Fetch(ref blob.Ref) (io.ReadCloser, uint32, error) {
	if b := f.CacheFetch(ref); b != nil {
		return b.Open(), b.Size(), nil
	}
	return f.FetcherEnumerator.Fetch(ref)
}

func (f *FetcherEnumerator) PrintStats() {
	var (
		start     = time.Now()
		then      = time.Now()
		lastStats stats
	)
	for _ = range time.Tick(10 * time.Second) {
		f.mu.Lock()
		now := time.Now()
		stats := f.stats
		f.mu.Unlock()

		elapsed := now.Sub(start)
		delta := uint64(now.Sub(then).Seconds())

		bytes := uint64(stats.bytes - lastStats.bytes)
		blobs := float64(stats.blobs - lastStats.blobs)
		hit := float64(stats.hit - lastStats.hit)
		miss := float64(stats.miss - lastStats.miss)

		fmt.Printf("%s: %s blobs (%s/sec); %s bytes (%s/sec); cache %s@%.0f%% (cum %.0f%%)\n",
			elapsed,
			humanize.SI(float64(stats.blobs), ""), humanize.SI(blobs/float64(delta), ""),
			humanize.Bytes(stats.bytes), humanize.Bytes(bytes/uint64(delta)),
			humanize.SI(hit+miss, ""), 100*hit/(hit+miss),
			float64(100*stats.hit)/float64(stats.hit+stats.miss),
		)

		lastStats = stats
		then = now
	}
}

func (f *FetcherEnumerator) Index(ch chan blobserver.BlobAndToken, dst *index.Index) {
	var long time.Duration
	for b := range ch {
		valid := b.ValidContents()

		f.mu.Lock()
		if valid {
			b := b
			f.Add(b.Blob)
		}
		f.stats.blobs++
		f.stats.bytes += uint64(b.Size())
		f.mu.Unlock()

		start := time.Now()
		r := b.Open()
		_, err := dst.ReceiveBlob(b.Ref(), r)
		if err != nil {
			log.Print(err)
		}
		r.Close()
		if elapsed := time.Now().Sub(start); elapsed > time.Second {
			long += elapsed
			fmt.Printf("elapsed %s to index sha1-%s at '%s' (cumulative %s)\n",
				elapsed, b.Ref().Digest(), b.Token, long)
			// pull some data out of the index to
			// describe blob? print continuation
			// token for easier restart?
		}
	}
}

func main() {
	flag.Parse()

	if len(*blobDir)*len(*indexDir) == 0 {
		flag.Usage()
		return
	}
	s, err := dir.New(*blobDir)
	if err != nil {
		log.Fatal(err)
	}
	src, ok := s.(blobserver.BlobStreamer)
	if !ok {
		log.Fatalf("%v is not a BlobStreamer", s)
	}

	db, err := leveldb.NewStorage(*indexDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	dst, err := index.New(db)
	if err != nil {
		log.Fatal(err)
	}

	fe := FetcherEnumerator{
		FetcherEnumerator: s,
		c1:                make(map[string]*blob.Blob),
		start:             time.Now(),
	}
	dst.InitBlobSource(&fe)

	ch := make(chan blobserver.BlobAndToken)
	go fe.PrintStats()
	for i := 0; i < *parallel; i++ {
		go fe.Index(ch, dst)
	}
	ctx := context.New()
	if err := src.StreamBlobs(ctx, ch, *streamStart); err != nil {
		log.Fatal(err)
	}
}
