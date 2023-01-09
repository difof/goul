package generics

import (
	"github.com/difof/goul/generics/containers"
	"testing"
)

func TestIterator_Close_SafeSlice(t *testing.T) {
	s := containers.NewSafeSlice(1, 2, 3, 4, 5, 6, 7)

	for i := 1; i <= 7; i++ {
		v := 0

		it := s.Iter()
		for item := range it.Next() {
			if item.Value() == i {
				it.Close()
				break
			}

			v = item.Value()
		}

		if v != i-1 {
			t.Fatalf("not closed: i=%d", i)
		}
	}
}

func TestIterator_Close_SafeMap(t *testing.T) {
	m := containers.NewSafeMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	it := m.Iter()
	for item := range it.Next() {
		if item.Value() == 3 {
			it.Close()
			break
		}

		t.Log(item.Key(), item.Value())
	}
}
