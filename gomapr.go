package gomapr

import (
	"errors"
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

func (r *Runner) ProcessMap(emitted interface{}, c chan struct{}) {
	key, mapped := r.MR.Map(emitted)
	r.Reduced.Add(key, mapped)
	c <- struct{}{}
}

func (r *Runner) ProcessReduce(key interface{}, partials []interface{}, c chan struct{}) {
	key, partial := r.MR.Reduce(key, partials)
	r.Reduced.Partials[key].Partials = []interface{}{partial}
	c <- struct{}{}
}

func (r *Runner) Run() error {
	c := make(chan struct{})
	emittedCount := 0
	for {
		emitted, err := r.MR.Emit()
		if err == EndOfEmit {
			break
		} else if err != nil {
			return err
		}
		emittedCount += 1
		go r.ProcessMap(emitted, c)
	}
	for i := 0; i < emittedCount; i++ {
		<-c
	}
	reducedCount := 0
	for key, partials := range r.Reduced.Partials {
		reducedCount += 1
		go r.ProcessReduce(key, partials.Partials, c)
	}
	for i := 0; i < reducedCount; i++ {
		<-c
	}
	return nil
}
