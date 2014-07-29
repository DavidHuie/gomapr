package gomapr

import (
	"sync"
	"testing"
)

type MRTest struct {
	current int
	Max     int
	mutex   *sync.Mutex
}

func NewMRTest() *MRTest {
	return &MRTest{1, 100, &sync.Mutex{}}
}

func (m *MRTest) Emit() (Event, error) {
	if m.current <= m.Max {
		defer func() {
			m.mutex.Lock()
			m.current += 1
			m.mutex.Unlock()
		}()
		return m.current, nil
	} else {
		return 0, EndOfEmit
	}
}

func (m *MRTest) Map(i Event) (ReduceKey, Partial) {
	if i.(int)%2 == 0 {
		return 2, 1
	}
	return 3, 1
}

func (m *MRTest) Reduce(key ReduceKey, values []Partial) (ReduceKey, Partial) {
	sum := 0
	for _, v := range values {
		sum += v.(int)
	}
	return key, sum
}

func TestMRTest(t *testing.T) {
	mrtest := NewMRTest()
	runner := NewRunner(mrtest)
	runner.Run(10)
	if runner.reduceWorkspace.groups[2].values[0] != 50 {
		t.Errorf("Invalid values")
	}
	if runner.reduceWorkspace.groups[3].values[0] != 50 {
		t.Errorf("Invalid values")
	}
}
