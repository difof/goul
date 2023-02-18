package containers

import "github.com/difof/goul/generics"

type ForwardLinkedListNode[V any] struct {
	value V
	next  *ForwardLinkedListNode[V]
}

// Value returns the value of the node.
func (n *ForwardLinkedListNode[V]) Value() V {
	return n.value
}

// Set sets the value of the node.
func (n *ForwardLinkedListNode[V]) Set(value V) {
	n.value = value
}

// Next returns the next node.
func (n *ForwardLinkedListNode[V]) Next() *ForwardLinkedListNode[V] {
	return n.next
}

// InsertAfter inserts a new node after the current node.
func (n *ForwardLinkedListNode[V]) InsertAfter(list *ForwardLinkedList[V], value V) *ForwardLinkedListNode[V] {
	newNode := &ForwardLinkedListNode[V]{value: value, next: n.next}
	n.next = newNode

	// check list's last node
	if list.last == n {
		list.last = newNode
	}

	list.size++

	return newNode
}

type ForwardLinkedList[V any] struct {
	first *ForwardLinkedListNode[V]
	last  *ForwardLinkedListNode[V]
	size  int
}

func NewForwardLinkedList[V any](values ...V) *ForwardLinkedList[V] {
	list := &ForwardLinkedList[V]{}

	for _, value := range values {
		list.Append(value)
	}

	return list
}

func CollectionAsForwardLinkedList[V any](c generics.Collection[int, V, Tuple[int, V]]) *ForwardLinkedList[V] {
	return c.(*ForwardLinkedList[V])
}

func (s *ForwardLinkedList[V]) First() *ForwardLinkedListNode[V] {
	return s.first
}

func (s *ForwardLinkedList[V]) Last() *ForwardLinkedListNode[V] {
	return s.last
}

func (s *ForwardLinkedList[V]) Len() int {
	return s.size
}

func (s *ForwardLinkedList[V]) Cap() int {
	return s.Len()
}

func (s *ForwardLinkedList[V]) IsEmpty() bool {
	return s.first == nil
}

// Get returns the value at the given index.
func (s *ForwardLinkedList[V]) Get(index int) V {
	if index < 0 || index >= s.size {
		panic("index out of range")
	}

	node := s.first
	for i := 0; i < index; i++ {
		node = node.next
	}

	return node.value
}

// GetNode returns the node at the given index.
func (s *ForwardLinkedList[V]) GetNode(index int) *ForwardLinkedListNode[V] {
	if index < 0 || index >= s.size {
		panic("index out of range")
	}

	node := s.first
	for i := 0; i < index; i++ {
		node = node.next
	}

	return node
}

func (s *ForwardLinkedList[V]) Values() []V {
	values := make([]V, s.size)
	node := s.first
	for i := 0; i < s.size; i++ {
		values[i] = node.value
		node = node.next
	}

	return values
}

func (s *ForwardLinkedList[V]) Set(index int, v V) {
	if index < 0 || index >= s.size {
		panic("index out of range")
	}

	node := s.first
	for i := 0; i < index; i++ {
		node = node.next
	}

	node.value = v
}

func (s *ForwardLinkedList[V]) SetElem(elem Tuple[int, V]) {
	s.Set(elem.Index(), elem.Value())
}

func (s *ForwardLinkedList[V]) Delete(index int) {
	if index < 0 || index >= s.size {
		panic("index out of range")
	}

	if index == 0 {
		s.first = s.first.next
		s.size--
		return
	}

	prev := s.first
	for i := 0; i < index-1; i++ {
		prev = prev.next
	}

	prev.next = prev.next.next
	s.size--
}

func (s *ForwardLinkedList[V]) Clear() {
	s.first = nil
	s.last = nil
	s.size = 0
}

// Append adds a new node to the end of the list.
func (s *ForwardLinkedList[V]) Append(v V) {
	node := &ForwardLinkedListNode[V]{value: v}

	if s.first == nil {
		s.first = node
		s.last = node
	} else {
		s.last.next = node
		s.last = node
	}

	s.size++
}

func (s *ForwardLinkedList[V]) AppendElem(elem Tuple[int, V]) {
	s.Append(elem.Value())
}

func (s *ForwardLinkedList[V]) Iter() *generics.Iterator[Tuple[int, V]] {
	return generics.NewIterator[Tuple[int, V]](s)
}

func (s *ForwardLinkedList[V]) IterHandler(iter *generics.Iterator[Tuple[int, V]]) {
	go func() {
		node := s.first
		for i := 0; i < s.size; i++ {
			select {
			case <-iter.Done():
				return
			case iter.NextChannel() <- NewTuple(i, node.value):
			}

			node = node.next
		}

		iter.IterationDone()
	}()
}

func (s *ForwardLinkedList[V]) AsIterable() generics.Iterable[Tuple[int, V]] {
	return s
}

func (s *ForwardLinkedList[V]) Factory() generics.Collection[int, V, Tuple[int, V]] {
	return NewForwardLinkedList[V]()
}

func (s *ForwardLinkedList[V]) FactoryFrom(values []V) generics.Collection[int, V, Tuple[int, V]] {
	return NewForwardLinkedList[V](values...)
}

func (s *ForwardLinkedList[V]) Clone() generics.Collection[int, V, Tuple[int, V]] {
	clone := NewForwardLinkedList[V]()
	for _, v := range s.Values() {
		clone.Append(v)
	}

	return clone
}

func (s *ForwardLinkedList[V]) AsCollection() generics.Collection[int, V, Tuple[int, V]] {
	return s
}
