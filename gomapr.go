package gomapr

import (
	"errors"
	"log"
	"sync"
)

var (
	EndOfEmit = errors.New("Nothing left to emit")
)

type MapReduce interface {
	Emit() (interface{}, error)
	Map(interface{}) (interface{}, interface{})
	Reduce(interface{}, []interface{}) (interface{}, interface{})
}

type Partials struct {
	Partials []interface{}
	mutex    *sync.Mutex
}

func NewPartials() *Partials {
	return &Partials{
		Partials: make([]interface{}, 0),
		mutex:    &sync.Mutex{},
	}
}

func (p *Partials) Add(v interface{}) {
	p.mutex.Lock()
	p.Partials = append(p.Partials, v)
	p.mutex.Unlock()
}

type Reduced struct {
	Partials map[interface{}]*Partials
	mutex    *sync.Mutex
}

func NewReduced() *Reduced {
	return &Reduced{
		make(map[interface{}]*Partials),
		&sync.Mutex{},
	}
}

func (r *Reduced) GetPartials(key interface{}) *Partials {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	partials, ok := r.Partials[key]
	if !ok {
		partials = NewPartials()
		r.Partials[key] = partials
	}
	return partials
}

func (r *Reduced) Add(key interface{}, value interface{}) {
	partials := r.GetPartials(key)
	partials.Add(value)
}

type Runner struct {
	Reduced *Reduced
	MR      MapReduce
}

func NewRunner(m MapReduce) *Runner {
	return &Runner{
		Reduced: NewReduced(),
		MR:      m,
	}
}

func (r *Runner) MapWorker(emitted chan interface{}, w *sync.WaitGroup) {
	for val := range emitted {
		key, mapped := r.MR.Map(val)
		r.Reduced.Add(key, mapped)
		w.Add(1)
		go r.Reduce(key, w)
	}
	w.Done()
}

func (r *Runner) Reduce(key interface{}, w *sync.WaitGroup) {
	r.Reduced.mutex.Lock()
	partials := r.Reduced.Partials[key]
	r.Reduced.mutex.Unlock()

	partials.mutex.Lock()
	defer partials.mutex.Unlock()
	if len(partials.Partials) > 1 {
		key, partial := r.MR.Reduce(key, partials.Partials)
		r.Reduced.Partials[key].Partials = []interface{}{partial}
	}
	w.Done()
}

func (r *Runner) Run(mappers int) {
	emit := make(chan interface{}, mappers)
	wg := sync.WaitGroup{}

	// Create background mapping workers.
	for i := 0; i < mappers; i++ {
		wg.Add(1)
		go r.MapWorker(emit, &wg)
	}

	// Emit all events for mapping.
	go func() {
		for {
			emitted, err := r.MR.Emit()
			if err == EndOfEmit {
				break
			} else if err != nil {
				log.Printf("Error emitting: %v", err)
				break
			}
			emit <- emitted
		}
		close(emit)
	}()

	// Wait for all mapping workers to finish.
	wg.Wait()
}
