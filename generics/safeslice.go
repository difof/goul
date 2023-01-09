package generics

import (
	"sync"
)

// SafeSlice is a thread-safe slice.
//
// It doesn't check out of bounds access.
type SafeSlice[V any] struct {
	slice []V
	lock  sync.RWMutex
}

// NewSafeSlice creates a new SafeSlice.
func NewSafeSlice[V any](items ...V) *SafeSlice[V] {
	if items != nil {
		return &SafeSlice[V]{slice: items}
	}

	return &SafeSlice[V]{slice: make([]V, 0)}
}

// NewSafeSliceN creates a new SafeSlice with size and capacity of n.
func NewSafeSliceN[V any](s, n int) *SafeSlice[V] {
	return &SafeSlice[V]{slice: make([]V, s, n)}
}

// Append appends a value to the slice.
func (s *SafeSlice[V]) Append(item V) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.slice = append(s.slice, item)
}

// AppendSlice appends a slice to the slice.
func (s *SafeSlice[V]) AppendSlice(slice []V) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.slice = append(s.slice, slice...)
}

// AppendSafeSlice appends a SafeSlice to the slice.
func (s *SafeSlice[V]) AppendSafeSlice(other *SafeSlice[V]) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.slice = append(s.slice, other.slice...)
}

// Get gets a value from the slice.
func (s *SafeSlice[V]) Get(i int) V {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.slice[i]
}

// Set sets a value in the slice.
func (s *SafeSlice[V]) Set(i int, item V) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.slice[i] = item
}

// Delete deletes a value at given index from the slice.
func (s *SafeSlice[V]) Delete(i int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.slice = append(s.slice[:i], s.slice[i+1:]...)
}

// Len returns the length of the slice.
func (s *SafeSlice[V]) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return len(s.slice)
}

// Cap returns the capacity of the slice.
func (s *SafeSlice[V]) Cap() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return cap(s.slice)
}

// IsEmpty returns true if the slice is empty.
func (s *SafeSlice[V]) IsEmpty() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return len(s.slice) == 0
}

// Iter returns iterator channel.
func (s *SafeSlice[V]) Iter() <-chan Tuple[int, V] {
	ch := make(chan Tuple[int, V])

	go func() {
		s.lock.RLock()
		defer s.lock.RUnlock()

		for i, item := range s.slice {
			ch <- NewTuple(i, item)
		}

		close(ch)
	}()

	return ch
}

// Clear clears the slice.
func (s *SafeSlice[V]) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.slice = make([]V, 0)
}

// Clone returns a clone of the slice. Same as Values.
func (s *SafeSlice[V]) Clone() Collection[int, V, Tuple[int, V]] {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return NewSafeSlice(s.slice...)
}

func (s *SafeSlice[V]) Collection() Collection[int, V, Tuple[int, V]] {
	return s
}

// Values returns all values in the slice. Same as Clone.
func (s *SafeSlice[V]) Values() []V {
	return s.Clone().(*SafeSlice[V]).slice
}

// Compare compares two slice items.
func (s *SafeSlice[V]) Compare(i, j Tuple[int, V], comp func(V, V) CompareResult) CompareResult {
	return comp(s.Get(i.Key()), s.Get(j.Key()))
}
