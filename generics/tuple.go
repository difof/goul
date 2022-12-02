package generics

type Tuple[T any, U any] struct {
	First  T
	Second U
}

// NewTuple creates a new Tuple.
func NewTuple[T any, U any](first T, second U) Tuple[T, U] {
	return Tuple[T, U]{first, second}
}
