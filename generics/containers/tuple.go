package containers

type Tuple[T any, U any] struct {
	First  T
	Second U
}

// NewTuple creates a new Tuple.
func NewTuple[T any, U any](first T, second U) Tuple[T, U] {
	return Tuple[T, U]{first, second}
}

// Key returns the first value of the tuple.
func (t Tuple[T, U]) Key() T {
	return t.First
}

// Index is same as Key().
func (t Tuple[T, U]) Index() T {
	return t.Key()
}

// Value returns the second value of the tuple.
func (t Tuple[T, U]) Value() U {
	return t.Second
}
