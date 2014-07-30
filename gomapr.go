package gomapr

import (
	"errors"
	"log"
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
	l      *sync.RWMutex
}

func newPartialGroup() *partialGroup {
	return &partialGroup{
		values: make([]Partial, 0),
		l:      &sync.RWMutex{},
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
	l      *sync.RWMutex
}

func newReduceWorkspace() *reduceWorkspace {
	return &reduceWorkspace{
		make(map[ReduceKey]*partialGroup),
		&sync.RWMutex{},
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
	wg              *sync.WaitGroup
}

func NewRunner(m MapReduce) *Runner {
	return &Runner{
		reduceWorkspace: newReduceWorkspace(),
		mr:              m,
		wg:              &sync.WaitGroup{},
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
		r.wg.Add(1)
		go r.reduce(key)
	}

	r.wg.Done()
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

	r.wg.Done()
}

// Starts the MapReduce task.
func (r *Runner) Run(mappers int) {
	emit := make(chan Event, mappers)

	// Create background mapping workers.
	for i := 0; i < mappers; i++ {
		r.wg.Add(1)
		go r.mapWorker(emit)
	}

	// Emit all events.
	go func() {
		for {
			emitted, err := r.mr.Emit()
			if err != EndOfEmit && err != nil {
				log.Printf("Error emitting: %v", err)
				break
			}

			emit <- emitted

			if err == EndOfEmit {
				break
			}

		}
		close(emit)
	}()

	r.wg.Wait()
}
