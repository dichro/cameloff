package fsck

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

type Parallel struct {
	Workers int
	wg      sync.WaitGroup
}

// Go starts the requisite number of goroutines calling f and
// immediately returns.
func (p *Parallel) Go(f func()) {
	p.wg.Add(p.Workers)
	for i := 0; i < p.Workers; i++ {
		go func() {
			defer p.wg.Done()
			f()
		}()
	}
}

func (p *Parallel) Wait() {
	p.wg.Wait()
}

func (p Parallel) String() string {
	return fmt.Sprintf("%d", p.Workers)
}

func (p Parallel) Set(val string) (err error) {
	p.Workers, err = strconv.Atoi(val)
	if err == nil && p.Workers < 1 {
		err = errors.New("value must be >= 1")
	}
	return
}
