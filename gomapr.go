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

func (r *Runner) MapWorker(emitted chan interface{}, response chan interface{}, status chan struct{}) {
	for val := range emitted {
		key, mapped := r.MR.Map(val)
		r.Reduced.Add(key, mapped)
		response <- key
	}
	status <- struct{}{}
}

func (r *Runner) Reduce(key interface{}) {
	partials := r.Reduced.Partials[key]
	partials.mutex.Lock()
	defer partials.mutex.Unlock()
	if len(partials.Partials) > 1 {
		key, partial := r.MR.Reduce(key, partials.Partials)
		r.Reduced.Partials[key].Partials = []interface{}{partial}
	}
}

func (r *Runner) Run(mappers int) {
	emit := make(chan interface{}, mappers)
	responses := make(chan interface{}, mappers)
	status := make(chan struct{})

	// Create background mapping workers.
	for i := 0; i < mappers; i++ {
		go r.MapWorker(emit, responses, status)
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
		log.Print("Closing channels")
		close(emit)
	}()

	// Wait for all mapping workers to finish.
	wg := sync.WaitGroup{}
	workersFinished := 0
	for {
		select {
		case <-status:
			workersFinished += 1
			if workersFinished == mappers {
				return
			}
		case key := <-responses:
			log.Printf("Reducing: %v", key)
			go r.Reduce(key)
			wg.Add(1)
		}
	}
	wg.Wait()
}
