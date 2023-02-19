package containers

import (
	"testing"
)

func TestIterator_Close_Slice(t *testing.T) {
	s := NewSlice(1, 2, 3, 4, 5, 6, 7)

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
