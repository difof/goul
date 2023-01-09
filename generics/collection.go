package generics

import "errors"

var ErrNotComparable = errors.New("collection not comparable")
var ErrNotFound = errors.New("item not found")

type CompareResult int

const (
	LessThan CompareResult = iota
	EqualTo
	GreaterThan
)

type Comparable[V, Iter any] interface {
	Compare(Iter, Iter, func(V, V) CompareResult) CompareResult
}

// NumericComparator is a comparator for numeric types included in LTGTConstraint.
func NumericComparator[V LTGTConstraint](a, b V) CompareResult {
	switch {
	case a == b:
		return EqualTo
	case a < b:
		return LessThan
	default:
		return GreaterThan
	}
}

// StringComparator is a comparator for strings.
func StringComparator(a, b string) CompareResult {
	switch {
	case a == b:
		return EqualTo
	case a < b:
		return LessThan
	default:
		return GreaterThan
	}
}

type Iterable[T any] interface {
	Iter() <-chan T
}

type Iterator[K, V any] struct {
	stop  chan struct{}
	ch    chan Tuple[K, V]
	owner Collection[K, V, Iterator[K, V]]
}

func NewIterator[K, V any](owner Collection[K, V, Iterator[K, V]]) Iterator[K, V] {
	return Iterator[K, V]{
		stop:  make(chan struct{}),
		ch:    make(chan Tuple[K, V]),
		owner: owner,
	}
}

func (i *Iterator[K, V]) Close() {
	i.stop <- struct{}{}
}

type Sizable interface {
	Len() int
	Cap() int
	IsEmpty() bool
}

type Gettable[K, V any] interface {
	Get(K) V
	Values() []V
}

type Settable[K, V any] interface {
	Set(K, V)
	Delete(K)
	Clear()
}

// Collection is a generic interface for collections.
// It is implemented by all collections in this package.
//
// The generic parameters are:
//
//	K: the key type
//	V: the value type
//	Iter: the iterator type which is usually a Tuple[K, V]
type Collection[K, V any, Iter any] interface {
	Sizable
	Iterable[Iter]
	Gettable[K, V]
	Settable[K, V]

	Clone() Collection[K, V, Iter]
	Collection() Collection[K, V, Iter]
}

// Any returns true if any item in the collection matches the predicate.
func Any[K, V any, Iter any](c Collection[K, V, Iter], fn func(Iter) (bool, error)) (bool, error) {
	for item := range c.Iter() {
		ok, err := fn(item)
		if err != nil {
			return false, err
		}

		if ok {
			return true, nil
		}
	}

	return false, nil
}

// All returns true if all items in the collection match the predicate.
func All[K, V any, Iter any](c Collection[K, V, Iter], fn func(Iter) (bool, error)) (bool, error) {
	for item := range c.Iter() {
		ok, err := fn(item)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

// Min returns the minimum item in the collection.
func Min[K, V any, Iter any](c Collection[K, V, Iter], comparator func(V, V) CompareResult) (min Iter, err error) {
	if comp, ok := c.(Comparable[V, Iter]); !ok {
		err = ErrNotComparable
		return
	} else {
		first := false

		for item := range c.Iter() {
			if !first {
				min = item
				first = true
				continue
			}

			if comp.Compare(min, item, comparator) == GreaterThan {
				min = item
			}
		}

		return
	}
}

// Max returns the maximum item in the collection.
func Max[K, V any, Iter any](c Collection[K, V, Iter], comparator func(V, V) CompareResult) (max Iter, err error) {
	if comp, ok := c.(Comparable[V, Iter]); !ok {
		err = ErrNotComparable
		return
	} else {
		first := false

		for item := range c.Iter() {
			if !first {
				max = item
				first = true
				continue
			}

			if comp.Compare(max, item, comparator) == LessThan {
				max = item
			}
		}

		return
	}
}

// First returns the first item in the collection.
func First[K, V any, Iter any](c Collection[K, V, Iter]) (first Iter, err error) {
	if !c.IsEmpty() {
		first = <-c.Iter()
	} else {
		err = ErrNotFound
	}

	return
}

// Last returns the last item in the collection.
func Last[K, V any, Iter any](c Collection[K, V, Iter]) (last Iter, err error) {
	if !c.IsEmpty() {
		// TODO: it is not the best idea to iterate over the whole collection
		for item := range c.Iter() {
			last = item
		}
	} else {
		err = ErrNotFound
	}

	return
}

//// Skip returns a new collection with the first n items skipped.
//func Skip[K, V any, Iter any](c Collection[K, V, Iter], n int) (r Collection[K, V, Iter]) {
//
//}
//
//// OrderBy sorts the collection by the given key and returns a new collection.
//func OrderBy[K, V any, Iter any](c Collection[K, V, Iter], comparator func(V, V) CompareResult) (r Collection[K, V, Iter], err error) {
//	if comp, ok := c.(Comparable[K, Iter]); !ok {
//		err = ErrNotComparable
//		return
//	} else if !c.IsEmpty() {
//		r := c.Clone()
//
//		/*
//			for i := 0; i < len(slice); i++ {
//				for j := i + 1; j < len(slice); j++ {
//					if ok, err := fn(slice[i], slice[j]); err != nil {
//						return err
//					} else if ok {
//						slice[i], slice[j] = slice[j], slice[i]
//					}
//				}
//			}
//		*/
//
//		for i := 0; i < r.Len(); i++ {
//			for j := i + 1; j < r.Len(); j++ {
//				if comp.Compare(r.Get(i), r.Get(j), comparator) == GreaterThan {
//					clone[i], clone[j] = clone[j], clone[i]
//				}
//			}
//		}
//
//		r = clone
//	}
//
//	return
//}
