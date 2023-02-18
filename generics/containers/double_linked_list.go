package containers

import "github.com/difof/goul/generics"

type DoubleLinkedListNode[V any] struct {
	value V
	prev  *DoubleLinkedListNode[V]
	next  *DoubleLinkedListNode[V]
}

// Value returns the value of the node.
func (n *DoubleLinkedListNode[V]) Value() V {
	return n.value
}

// Set sets the value of the node.
func (n *DoubleLinkedListNode[V]) Set(value V) {
	n.value = value
}

// Prev returns the previous node.
func (n *DoubleLinkedListNode[V]) Prev() *DoubleLinkedListNode[V] {
	return n.prev
}

// Next returns the next node.
func (n *DoubleLinkedListNode[V]) Next() *DoubleLinkedListNode[V] {
	return n.next
}

// InsertAfter inserts a new node after the current node.
func (n *DoubleLinkedListNode[V]) InsertAfter(list *DoubleLinkedList[V], value V) *DoubleLinkedListNode[V] {
	newNode := &DoubleLinkedListNode[V]{value: value, prev: n, next: n.next}
	if n.next != nil {
		n.next.prev = newNode
	}
	n.next = newNode

	// check list's last node
	if list.last == n {
		list.last = newNode
	}

	list.size++

	return newNode
}

// InsertBefore inserts a new node before the current node.
func (n *DoubleLinkedListNode[V]) InsertBefore(list *DoubleLinkedList[V], value V) *DoubleLinkedListNode[V] {
	newNode := &DoubleLinkedListNode[V]{value: value, prev: n.prev, next: n}
	if n.prev != nil {
		n.prev.next = newNode
	}
	n.prev = newNode

	// check list's first node
	if list.first == n {
		list.first = newNode
	}

	list.size++

	return newNode
}

type DoubleLinkedList[V any] struct {
	first *DoubleLinkedListNode[V]
	last  *DoubleLinkedListNode[V]
	size  int
}

func NewDoubleLinkedList[V any](values ...V) *DoubleLinkedList[V] {
	list := &DoubleLinkedList[V]{}

	for _, value := range values {
		list.Append(value)
	}

	return list
}

func CollectionAsDoubleLinkedList[V any](c generics.Collection[int, V, Tuple[int, V]]) *DoubleLinkedList[V] {
	return c.(*DoubleLinkedList[V])
}

func (s *DoubleLinkedList[V]) First() *DoubleLinkedListNode[V] {
	return s.first
}

func (s *DoubleLinkedList[V]) Last() *DoubleLinkedListNode[V] {
	return s.last
}

func (s *DoubleLinkedList[V]) Len() int {
	return s.size
}

func (s *DoubleLinkedList[V]) Cap() int {
	return s.Len()
}

func (s *DoubleLinkedList[V]) IsEmpty() bool {
	return s.first == nil
}

// Get returns the value at the given index.
func (s *DoubleLinkedList[V]) Get(index int) V {
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
func (s *DoubleLinkedList[V]) GetNode(index int) *DoubleLinkedListNode[V] {
	if index < 0 || index >= s.size {
		panic("index out of range")
	}

	node := s.first
	for i := 0; i < index; i++ {
		node = node.next
	}

	return node
}

func (s *DoubleLinkedList[V]) Values() []V {
	values := make([]V, s.size)
	node := s.first
	for i := 0; i < s.size; i++ {
		values[i] = node.value
		node = node.next
	}

	return values
}

func (s *DoubleLinkedList[V]) Set(index int, v V) {
	if index < 0 || index >= s.size {
		panic("index out of range")
	}

	node := s.first
	for i := 0; i < index; i++ {
		node = node.next
	}

	node.value = v
}

func (s *DoubleLinkedList[V]) SetElem(elem Tuple[int, V]) {
	s.Set(elem.Index(), elem.Value())
}

// Delete removes the node at the given index.
func (s *DoubleLinkedList[V]) Delete(index int) {
	if index < 0 || index >= s.size {
		panic("index out of range")
	}

	node := s.first
	for i := 0; i < index; i++ {
		node = node.next
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		s.first = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		s.last = node.prev
	}

	s.size--
}

// DeleteFirst removes the first node.
func (s *DoubleLinkedList[V]) DeleteFirst() {
	if s.first == nil {
		return
	}

	if s.first.next != nil {
		s.first.next.prev = nil
	} else {
		s.last = nil
	}

	s.first = s.first.next
	s.size--
}

// DeleteLast removes the last node.
func (s *DoubleLinkedList[V]) DeleteLast() {
	if s.last == nil {
		return
	}

	if s.last.prev != nil {
		s.last.prev.next = nil
	} else {
		s.first = nil
	}

	s.last = s.last.prev
	s.size--
}

func (s *DoubleLinkedList[V]) Clear() {
	s.first = nil
	s.last = nil
	s.size = 0
}

// Prepend adds a new node to the beginning of the list.
func (s *DoubleLinkedList[V]) Prepend(v V) {
	newNode := &DoubleLinkedListNode[V]{value: v, next: s.first}
	if s.first != nil {
		s.first.prev = newNode
	}
	s.first = newNode

	if s.last == nil {
		s.last = newNode
	}

	s.size++
}

// Append adds a new node to the end of the list.
func (s *DoubleLinkedList[V]) Append(v V) {
	newNode := &DoubleLinkedListNode[V]{value: v, prev: s.last}
	if s.last != nil {
		s.last.next = newNode
	}
	s.last = newNode

	if s.first == nil {
		s.first = newNode
	}

	s.size++
}

func (s *DoubleLinkedList[V]) AppendElem(elem Tuple[int, V]) {
	s.Append(elem.Value())
}

func (s *DoubleLinkedList[V]) Iter() *generics.Iterator[Tuple[int, V]] {
	return generics.NewIterator[Tuple[int, V]](s)
}

func (s *DoubleLinkedList[V]) IterHandler(iter *generics.Iterator[Tuple[int, V]]) {
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

func (s *DoubleLinkedList[V]) AsIterable() generics.Iterable[Tuple[int, V]] {
	return s
}

func (s *DoubleLinkedList[V]) Factory() generics.Collection[int, V, Tuple[int, V]] {
	return NewForwardLinkedList[V]()
}

func (s *DoubleLinkedList[V]) FactoryFrom(values []V) generics.Collection[int, V, Tuple[int, V]] {
	return NewForwardLinkedList[V](values...)
}

func (s *DoubleLinkedList[V]) Clone() generics.Collection[int, V, Tuple[int, V]] {
	clone := NewForwardLinkedList[V]()
	for _, v := range s.Values() {
		clone.Append(v)
	}

	return clone
}

func (s *DoubleLinkedList[V]) AsCollection() generics.Collection[int, V, Tuple[int, V]] {
	return s
}
