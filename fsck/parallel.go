package fsck

import "sync"

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
