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

func (r *Runner) Run() error {
	for {
		emitted, err := r.MR.Emit()
		if err == EndOfEmit {
			break
		} else if err != nil {
			return err
		}
		key, mapped := r.MR.Map(emitted)
		r.Reduced.Add(key, mapped)

	}
	for key, partials := range r.Reduced.Partials {
		key, partial := r.MR.Reduce(key, partials.Partials)
		r.Reduced.Partials[key].Partials = []interface{}{partial}
	}
	return nil
}
