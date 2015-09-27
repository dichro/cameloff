package fsck

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

type Stats struct {
	mu     sync.Mutex
	counts map[string]int
}

func NewStats() *Stats {
	return &Stats{counts: make(map[string]int)}
}

func (s *Stats) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := []string{}
	for t, c := range s.counts {
		parts = append(parts, fmt.Sprintf("%s: %d", t, c))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func (s *Stats) Add(entry string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[entry]++
}

func (s *Stats) LogEvery(interval time.Duration) *time.Ticker {
	t := time.NewTicker(interval)
	go func() {
		for _ = range t.C {
			log.Print(s)
		}
	}()
	return t
}

func (s *Stats) entries() entries {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := make(entries, 0, len(s.counts))
	for k, v := range s.counts {
		e = append(e, entry{k, v})
	}
	return e
}

func (s *Stats) LogTopNEvery(n int, interval time.Duration) *time.Ticker {
	t := time.NewTicker(interval)
	go func() {
		for _ = range t.C {
			e := s.entries()
			sort.Sort(byValue(e))
			log.Print(e[:n])
		}
	}()
	return t
}

type entry struct {
	key string
	val int
}

func (e entry) String() string { return fmt.Sprintf("%q: %d", e.key, e.val) }

type entries []entry

func (e entries) String() string {
	s := make([]string, len(e))
	for i, e := range e {
		s[i] = e.String()
	}
	return strings.Join(s, ", ")
}

type byValue []entry

func (b byValue) Len() int           { return len(b) }
func (b byValue) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byValue) Less(i, j int) bool { return b[j].val < b[i].val }
