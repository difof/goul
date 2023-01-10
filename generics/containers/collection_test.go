package containers

import (
	"github.com/difof/goul/generics"
	"strings"
	"testing"
)

func TestAny(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	if found, _ := generics.Any(slice.AsIterable(), func(i Tuple[int, int]) (bool, error) {
		return i.Value() == 5, nil
	}); !found {
		t.Fatal("not found")
	}

	m := NewSafeMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	if found, _ := generics.Any(m.AsIterable(), func(kv Tuple[string, int]) (bool, error) {
		return kv.Value() == 2, nil
	}); !found {
		t.Fatal("not found")
	}
}

func TestAll(t *testing.T) {
	// odd numbers
	slice := NewSafeSlice(1, 3, 5, 7, 9)

	if all, _ := generics.All(slice.AsIterable(), func(i Tuple[int, int]) (bool, error) {
		return i.Value()%2 == 1, nil
	}); !all {
		t.Fatal("not all odd")
	}
}

func TestMinMax(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 10, 7, 8, 9, 6)

	if min, err := generics.Min(slice.AsIterable(), generics.NumericComparator[int]); err != nil || min.Value() != 1 {
		t.Fatal(min, err)
	}

	if max, err := generics.Max(slice.AsIterable(), generics.NumericComparator[int]); err != nil || max.Value() != 10 {
		t.Fatal(max, err)
	}
}

func TestFirst(t *testing.T) {
	slice := NewSafeSlice(11, 2, 3, 4, 5, 10, 7, 8, 9, 6)

	if first, err := generics.First(slice.AsCollection()); err != nil || first.Value() != 11 {
		t.Fatal(first, err)
	}
}

func TestLast(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 10, 7, 8, 9, 6)

	if last, err := generics.Last(slice.AsCollection()); err != nil || last.Value() != 6 {
		t.Fatal(last, err)
	}
}

func TestOrderBy(t *testing.T) {
	slice := NewSafeSlice(4, 2, 3, 1, 5, 10, 7, 8, 9, 6)

	ordered := generics.OrderBy(slice.AsCollection(), generics.NumericComparator[int])

	if ordered.Values()[0] != 1 {
		t.Fatal("not ordered")
	}

	if ordered.Values()[9] != 10 {
		t.Fatal("not ordered")
	}

	generics.Each(ordered.AsIterable(), func(i Tuple[int, int]) error {
		t.Log(i.Key(), i.Value())
		return nil
	})
}

func TestFind(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 10, 7, 8, 9, 6, 5)

	if found, err := generics.Find(slice.AsIterable(), func(i Tuple[int, int]) (bool, error) {
		return i.Value() == 5, nil
	}); err != nil || found.Value() != 5 || found.Key() != 4 {
		t.Fatal(found, err)
	}
}

func TestFindLast(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 10, 7, 8, 9, 6, 5)

	if found, err := generics.FindLast(slice.AsIterable(), func(i Tuple[int, int]) (bool, error) {
		return i.Value() == 5, nil
	}); err != nil || found.Value() != 5 || found.Key() != 10 {
		t.Fatal(found, err)
	}
}

func TestSelect(t *testing.T) {
	m := NewSafeMap[string, int](
		NewTuple("a", 1),
		NewTuple("b", 2),
		NewTuple("c", 3))

	n := NewSafeMap[string, float32]()

	mapped, err := generics.Select(m.AsCollection(), n.AsCollection(), func(kv Tuple[string, int]) (Tuple[string, float32], error) {
		return NewTuple(strings.ToUpper(kv.Key()), float32(kv.Value())*2.001), nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if mapped.Len() != 3 {
		t.Fatal("not mapped")
	}

	for item := range n.Iter().Next() {
		t.Log(item.Key(), item.Value())
	}
}

func TestWhere(t *testing.T) {
	slice := NewSafeSlice(1, 2, 3, 4, 5, 10, 7, 8, 9, 6, 5)

	filtered, err := generics.Where(slice.AsCollection(), func(i Tuple[int, int]) (bool, error) {
		return i.Value() > 5, nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if filtered.Len() != 5 {
		t.Fatal("not filtered")
	}

	for item := range filtered.Iter().Next() {
		t.Log(item.Key(), item.Value())
	}
}
