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
	return &MRTest{1, 1000000, &sync.Mutex{}}
}

func (m *MRTest) Emit() (Event, error) {
	if m.current < m.Max {
		defer func() {
			m.mutex.Lock()
			m.current += 1
			m.mutex.Unlock()
		}()
		return m.current, nil
	} else {
		return m.Max, EndOfEmit
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
	runner := NewRunner(mrtest, 10, .5)

	runner.Run()

	groups := runner.Groups()

	if groups[2] != 500000 {
		t.Errorf("Invalid values: %v", groups[2])
	}
	if groups[3] != 500000 {
		t.Errorf("Invalid values: %v", groups[3])
	}
}
