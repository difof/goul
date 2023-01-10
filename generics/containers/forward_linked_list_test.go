package containers

import (
	"github.com/difof/goul/generics"
	"testing"
)

func TestForwardLinkedList_Iter(t *testing.T) {
	list := NewForwardLinkedList[int]()
	list.Append(1)
	list.Append(4)
	list.Append(2)
	list.Append(3)
	list.Append(5)

	list = CollectionAsForwardLinkedList(generics.OrderBy(list.AsCollection(), generics.NumericComparator[int]))

	generics.Each(list.AsIterable(), func(i Tuple[int, int]) error {
		t.Log(i.Value())
		return nil
	})
}
