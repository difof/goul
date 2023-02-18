package generics

type Iterable[T any] interface {
	// Iter returns an iterator for the collection.
	Iter() *Iterator[T]

	// IterHandler is the iterator handler goroutine.
	IterHandler(*Iterator[T])

	// AsIterable returns the iterator type.
	AsIterable() Iterable[T]
}

// Iterator is a generic iterator used to iterate over Collection.
// Use Iterator.Close for early loop termination.
type Iterator[T any] struct {
	stop chan struct{}
	ch   chan T
	Args []any
	err  error
}

// NewIterator returns a new iterator. Used by owner.Iter().
func NewIterator[T any](owner Iterable[T], args ...any) (it *Iterator[T]) {
	it = &Iterator[T]{
		stop: make(chan struct{}, 1),
		ch:   make(chan T, 1),
		Args: args,
	}

	owner.IterHandler(it)

	return
}

// Close stops the iterator. Used to stop iteration early.
func (it *Iterator[T]) Close() {
	it.stop <- struct{}{}
}

// SetError sets the error on the iterator.
// Used by Iterable.IterHandler to signal an error.
func (it *Iterator[T]) SetError(err error) {
	it.err = err
}

// Error returns the error on the iterator.
// Used by Iterable.IterHandler to signal an error.
func (it *Iterator[T]) Error() error {
	return it.err
}

// IterationDone closes the iterator channel.
// Used by Iterable.IterHandler to signal that iteration is done.
func (it *Iterator[T]) IterationDone() {
	close(it.ch)
}

// Next returns the next item in the iterator.
//
// Each time Next is called, current item will be returned and goroutine will read the next item.
// This might be costly for collections that a single iteration is heavy.
func (it *Iterator[T]) Next() <-chan T {
	return it.ch
}

// Done returns a channel that is closed when the iterator is done.
// used by Iterable.IterHandler to stop iteration.
func (it *Iterator[T]) Done() <-chan struct{} {
	return it.stop
}

// NextChannel returns the receiving channel to be returned by Next().
// used by Iterable.IterHandler to send items.
func (it *Iterator[T]) NextChannel() chan<- T {
	return it.ch
}
