package containers

type Tuple[K any, V any] struct {
	First  K
	Second V
}

// NewTuple creates a new Tuple.
func NewTuple[K any, V any](first K, second V) Tuple[K, V] {
	return Tuple[K, V]{first, second}
}

// Key returns the first value of the tuple.
func (t Tuple[K, V]) Key() K {
	return t.First
}

// Index is same as Key().
func (t Tuple[K, V]) Index() K {
	return t.Key()
}

// Value returns the second value of the tuple.
func (t Tuple[K, V]) Value() V {
	return t.Second
}
