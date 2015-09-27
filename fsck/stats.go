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

func (s Stats) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := []string{}
	for t, c := range s.counts {
		parts = append(parts, fmt.Sprintf("%s: %d", t, c))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func (s Stats) Add(entry string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[entry]++
}

func (s Stats) LogEvery(interval time.Duration) *time.Ticker {
	t := time.NewTicker(interval)
	go func() {
		for _ = range t.C {
			log.Print(s)
		}
	}()
	return t
}
