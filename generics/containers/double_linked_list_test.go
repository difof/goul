package containers

import (
	"github.com/difof/goul/generics"
	"testing"
)

func TestDoubleLinkedList_Iter(t *testing.T) {
	list := NewDoubleLinkedList[int](2, 3, 5, 6, 7, 8, 9, 0)

	found, err := generics.Find(list.AsIterable(), func(i Tuple[int, int]) (bool, error) {
		return i.Value() == 6, nil
	})

	if err == generics.ErrNotFound {
		t.Fatalf("failed to find value: %v", err)
	}

	t.Logf("found: %v", found)
}
