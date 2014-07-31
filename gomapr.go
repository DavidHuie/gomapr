package gomapr

import (
	"errors"
	"log"
	"math/rand"
	"sync"
)

var (
	EndOfEmit = errors.New("Nothing left to emit")
)

type Event interface{}
type Partial interface{}
type ReduceKey interface{}

type MapReduce interface {
	Emit() (Event, error)
	Map(Event) (ReduceKey, Partial)
	Reduce(ReduceKey, []Partial) (ReduceKey, Partial)
}

// Corresponds to a set of values that a reducer can join.
type partialGroup struct {
	values []Partial
	l      *sync.Mutex
}

func newPartialGroup() *partialGroup {
	return &partialGroup{
		values: make([]Partial, 0),
		l:      &sync.Mutex{},
	}
}

// Adds a value to the group.
func (p *partialGroup) add(v interface{}) {
	p.values = append(p.values, v)
}

// Replaces the contents of the partial group.
func (p *partialGroup) replace(v interface{}) {
	p.values = []Partial{v}
}

// Contains all partial groups.
type reduceWorkspace struct {
	groups map[ReduceKey]*partialGroup
	l      *sync.Mutex
}

func newReduceWorkspace() *reduceWorkspace {
	return &reduceWorkspace{
		make(map[ReduceKey]*partialGroup),
		&sync.Mutex{},
	}
}

// Returns a partial group by its key.
func (r *reduceWorkspace) getPartialGroup(key ReduceKey) *partialGroup {
	r.l.Lock()
	defer r.l.Unlock()

	partialGroup, ok := r.groups[key]
	if !ok {
		partialGroup = newPartialGroup()
		r.groups[key] = partialGroup
	}

	return partialGroup
}

// Adds a key-value pair to its appropriate partial group.
func (r *reduceWorkspace) add(key ReduceKey, value Partial) {
	partialGroup := r.getPartialGroup(key)

	partialGroup.l.Lock()
	partialGroup.add(value)
	partialGroup.l.Unlock()
}

// Replaces an existing partial group with the input arguments.
func (r *reduceWorkspace) replace(key ReduceKey, value Partial) {
	partialGroup := r.getPartialGroup(key)

	partialGroup.l.Lock()
	partialGroup.replace(value)
	partialGroup.l.Unlock()
}

// Contains configuration for a MapReduce task.
type Runner struct {
	reduceWorkspace *reduceWorkspace
	mr              MapReduce
	mapWg           *sync.WaitGroup
	reduceWg        *sync.WaitGroup
	mappers         int
	reduceFactor    float64
	unreduced       map[ReduceKey]struct{}
	unreducedL      *sync.Mutex
}

func NewRunner(mr MapReduce, mappers int, reduceFactor float64) *Runner {
	if reduceFactor < 0 || reduceFactor > 1 {
		panic("Invalid reduce factor")
	}

	return &Runner{
		reduceWorkspace: newReduceWorkspace(),
		mr:              mr,
		mapWg:           &sync.WaitGroup{},
		reduceWg:        &sync.WaitGroup{},
		mappers:         mappers,
		reduceFactor:    reduceFactor,
		unreduced:       make(map[ReduceKey]struct{}),
		unreducedL:      &sync.Mutex{},
	}
}

// Returns the map containing all groups. Only safe to call after
// the task has completed.
func (r *Runner) Groups() map[ReduceKey]Partial {
	r.reduceWorkspace.l.Lock()
	defer r.reduceWorkspace.l.Unlock()

	groups := make(map[ReduceKey]Partial)

	for k, v := range r.reduceWorkspace.groups {
		groups[k] = v.values[0]
	}

	return groups
}

// Maps the input it receives on its emitted channel, spawning
// a reduce task when appropriate.
func (r *Runner) mapWorker(emitted chan Event) {
	for val := range emitted {
		key, mapped := r.mr.Map(val)
		r.reduceWorkspace.add(key, mapped)

		// Launch reduce step probabilistically.
		if rand.Float64() < r.reduceFactor {
			r.unreducedL.Lock()
			delete(r.unreduced, key)
			r.unreducedL.Unlock()

			r.reduceWg.Add(1)
			go r.reduce(key)
		} else {
			r.unreducedL.Lock()
			r.unreduced[key] = struct{}{}
			r.unreducedL.Unlock()
		}
	}

	r.mapWg.Done()
}

// Reduces the partial group with the matching input key.
func (r *Runner) reduce(key ReduceKey) {
	partialGroup := r.reduceWorkspace.getPartialGroup(key)

	partialGroup.l.Lock()
	defer partialGroup.l.Unlock()

	if len(partialGroup.values) > 1 {
		newKey, partial := r.mr.Reduce(key, partialGroup.values)

		if key == newKey {
			partialGroup.replace(partial)
		} else {
			r.reduceWorkspace.replace(key, partial)
		}
	}

	r.reduceWg.Done()
}

// Starts the MapReduce task.
func (r *Runner) Run() {
	events := make(chan Event, r.mappers)

	// Create background mapping workers.
	for i := 0; i < r.mappers; i++ {
		r.mapWg.Add(1)
		go r.mapWorker(events)
	}

	// Emit all events.
	go func() {
		for {
			event, err := r.mr.Emit()
			if err != EndOfEmit && err != nil {
				log.Printf("Error emitting: %v", err)
				break
			}

			events <- event

			if err == EndOfEmit {
				break
			}

		}
		close(events)
	}()

	r.mapWg.Wait()

	// Reduce unreduced keys.
	for key, _ := range r.unreduced {
		r.reduceWg.Add(1)
		go r.reduce(key)
	}
	r.unreduced = make(map[ReduceKey]struct{})
	r.reduceWg.Wait()
}

// Runs MapReduce synchronously.
func (r *Runner) RunSynchronous() {
	for {
		event, err := r.mr.Emit()
		if err != EndOfEmit && err != nil {
			log.Printf("Error emitting: %v", err)
			break
		}

		key, mapped := r.mr.Map(event)
		r.reduceWorkspace.add(key, mapped)

		if err == EndOfEmit {
			break
		}

	}

	for key, _ := range r.reduceWorkspace.groups {
		r.reduceWg.Add(1)
		r.reduce(key)
	}
}
