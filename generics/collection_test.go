package generics

import "testing"

func TestAny(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	if found, _ := Any(slice.Collection(), func(i Tuple[int, int]) (bool, error) {
		return i.Value() == 5, nil
	}); !found {
		t.Fatal("not found")
	}

	m := NewSafeMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	if found, _ := Any(m.Collection(), func(kv Tuple[string, int]) (bool, error) {
		return kv.Value() == 2, nil
	}); !found {
		t.Fatal("not found")
	}
}

func TestAll(t *testing.T) {
	// odd numbers
	slice := NewSafeSlice(1, 3, 5, 7, 9)

	if all, _ := All(slice.Collection(), func(i Tuple[int, int]) (bool, error) {
		return i.Value()%2 == 1, nil
	}); !all {
		t.Fatal("not all odd")
	}
}

func TestMinMax(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	if min, err := Min(slice.Collection(), NumericComparator[int]); err != nil || min.Value() != 1 {
		t.Fatal(min, err)
	}

	if max, err := Max(slice.Collection(), NumericComparator[int]); err != nil || max.Value() != 10 {
		t.Fatal(max, err)
	}
}
