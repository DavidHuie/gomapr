package gomapr

import (
	"errors"
	"log"
	"reflect"
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

// Corresponds to a set of values that a reducer can join.
type partialGroup struct {
	values []interface{}
	mutex  *sync.Mutex
}

func newPartialGroup() *partialGroup {
	return &partialGroup{
		values: make([]interface{}, 0),
		mutex:  &sync.Mutex{},
	}
}

// Adds a value to the group.
func (p *partialGroup) add(v interface{}) {
	p.values = append(p.values, v)
}

// Replaces the contents of the partial group.
func (p *partialGroup) replace(v interface{}) {
	p.values = []interface{}{v}
}

// Contains all partial groups.
type reduceWorkspace struct {
	groups map[interface{}]*partialGroup
	mutex  *sync.Mutex
}

func newReduceWorkspace() *reduceWorkspace {
	return &reduceWorkspace{
		make(map[interface{}]*partialGroup),
		&sync.Mutex{},
	}
}

// Returns a partial group by its key.
func (r *reduceWorkspace) getPartialGroup(key interface{}) *partialGroup {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	partialGroup, ok := r.groups[key]
	if !ok {
		partialGroup = newPartialGroup()
		r.groups[key] = partialGroup
	}

	return partialGroup
}

// Adds a key-value pair to its appropriate partial group.
func (r *reduceWorkspace) add(key interface{}, value interface{}) {
	partialGroup := r.getPartialGroup(key)

	partialGroup.mutex.Lock()
	partialGroup.add(value)
	partialGroup.mutex.Unlock()
}

// Replaces an existing partial group with the input arguments.
func (r *reduceWorkspace) replace(key interface{}, value interface{}) {
	partialGroup := r.getPartialGroup(key)
	partialGroup.replace(value)
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

// Maps the input it receives on its emitted channel, spawning
// a reduce task when appropriate.
func (r *Runner) mapWorker(emitted chan interface{}) {
	for val := range emitted {
		key, mapped := r.mr.Map(val)
		r.reduceWorkspace.add(key, mapped)
		r.wg.Add(1)
		go r.reduce(key)
	}

	r.wg.Done()
}

// Reduces the partial group with the matching input key.
func (r *Runner) reduce(key interface{}) {
	partialGroup := r.reduceWorkspace.getPartialGroup(key)

	partialGroup.mutex.Lock()
	defer partialGroup.mutex.Unlock()

	if len(partialGroup.values) > 1 {
		newKey, partial := r.mr.Reduce(key, partialGroup.values)

		if reflect.DeepEqual(key, newKey) {
			partialGroup.replace(partial)
		} else {
			r.reduceWorkspace.replace(key, partial)
		}
	}

	r.wg.Done()
}

// Starts the MapReduce task.
func (r *Runner) Run(mappers int) {
	emit := make(chan interface{}, mappers)

	// Create background mapping workers.
	for i := 0; i < mappers; i++ {
		r.wg.Add(1)
		go r.mapWorker(emit)
	}

	// Emit all events.
	go func() {
		for {
			emitted, err := r.mr.Emit()
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

	r.wg.Wait()
}
