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
type PartialGroup struct {
	Values []interface{}
	mutex  *sync.Mutex
}

func NewPartialGroup() *PartialGroup {
	return &PartialGroup{
		Values: make([]interface{}, 0),
		mutex:  &sync.Mutex{},
	}
}

// Adds a value to the group.
func (p *PartialGroup) Add(v interface{}) {
	p.mutex.Lock()
	p.Values = append(p.Values, v)
	p.mutex.Unlock()
}

// Replaces the contents of the partial group.
func (p *PartialGroup) replace(v interface{}) {
	p.Values = []interface{}{v}
}

// Contains all partial groups.
type ReduceWorkspace struct {
	Groups map[interface{}]*PartialGroup
	mutex  *sync.Mutex
}

func NewReduceWorkspace() *ReduceWorkspace {
	return &ReduceWorkspace{
		make(map[interface{}]*PartialGroup),
		&sync.Mutex{},
	}
}

// Returns a partial group by its key.
func (r *ReduceWorkspace) GetPartialGroup(key interface{}) *PartialGroup {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	partialGroup, ok := r.Groups[key]
	if !ok {
		partialGroup = NewPartialGroup()
		r.Groups[key] = partialGroup
	}

	return partialGroup
}

// Adds a key-value pair to its appropriate partial group.
func (r *ReduceWorkspace) Add(key interface{}, value interface{}) {
	partialGroup := r.GetPartialGroup(key)
	partialGroup.Add(value)
}

// Replaces an existing partial group with the input arguments.
func (r *ReduceWorkspace) Replace(key interface{}, value interface{}) {
	partialGroup := r.GetPartialGroup(key)
	partialGroup.replace(value)
}

// Contains configuration for a MapReduce task.
type Runner struct {
	ReduceWorkspace *ReduceWorkspace
	MR              MapReduce
	wg              *sync.WaitGroup
}

func NewRunner(m MapReduce) *Runner {
	return &Runner{
		ReduceWorkspace: NewReduceWorkspace(),
		MR:              m,
		wg:              &sync.WaitGroup{},
	}
}

// Maps the input it receives on its emitted channel, spawning
// a reduce task when appropriate.
func (r *Runner) MapWorker(emitted chan interface{}) {
	for val := range emitted {
		key, mapped := r.MR.Map(val)
		r.ReduceWorkspace.Add(key, mapped)
		r.wg.Add(1)
		go r.Reduce(key)
	}

	r.wg.Done()
}

// Reduces the partial group with the matching input key.
func (r *Runner) Reduce(key interface{}) {
	partialGroup := r.ReduceWorkspace.GetPartialGroup(key)

	partialGroup.mutex.Lock()
	defer partialGroup.mutex.Unlock()

	if len(partialGroup.Values) > 1 {
		newKey, partial := r.MR.Reduce(key, partialGroup.Values)

		if reflect.DeepEqual(key, newKey) {
			partialGroup.replace(partial)
		} else {
			r.ReduceWorkspace.Replace(key, partial)
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
		go r.MapWorker(emit)
	}

	// Emit all events.
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

	r.wg.Wait()
}
