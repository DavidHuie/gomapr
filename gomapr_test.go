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

func (m *MRTest) Emit() (interface{}, error) {
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

func (m *MRTest) Map(i interface{}) (interface{}, interface{}) {
	if i.(int)%2 == 0 {
		return 2, 1
	}
	return 3, 1
}

func (m *MRTest) Reduce(key interface{}, values []interface{}) (interface{}, interface{}) {
	sum := 0
	for _, v := range values {
		sum += v.(int)
	}
	return key, sum
}

func TestMRTest(t *testing.T) {
	mrtest := NewMRTest()
	runner := NewRunner(mrtest)
	if err := runner.Run(); err != nil {
		t.Fatal(err)
	}
	if runner.Reduced.Partials[2].Partials[0] != 50 {
		t.Errorf("Invalid values")
	}
	if runner.Reduced.Partials[3].Partials[0] != 50 {
		t.Errorf("Invalid values")
	}
}
