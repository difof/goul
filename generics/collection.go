package generics

import "errors"

var ErrNotComparable = errors.New("collection not comparable")
var ErrNotFound = errors.New("item not found")

// Slicable is a collection that can be sliced.
type Slicable interface {
	Slice(start, count int) Slicable
}

// Sizable is a generic interface for collections that can be sized.
type Sizable interface {
	Len() int
	Cap() int
	IsEmpty() bool
}

// Gettable is a generic interface for collections that support reading.
type Gettable[K, V any] interface {
	Get(K) V
	Values() []V
}

// Settable is a generic interface for collections that support writing.
type Settable[K, V, Elem any] interface {
	Set(K, V)
	SetElem(Elem)
	Delete(K)
	Clear()
	AppendElem(Elem)
}

// Collection is a generic interface for collections.
// It is implemented by all collections in this package.
//
// The generic parameters are:
//
//		K: the key type
//		V: the value type
//	 Elem: iterator element type, usually a Tuple[K, V]
type Collection[K, V, Elem any] interface {
	Sizable
	Gettable[K, V]
	Settable[K, V, Elem]
	Iterable[Elem]

	// Factory returns a new collection of the same type.
	Factory() Collection[K, V, Elem]

	// Clone returns a copy of the collection.
	Clone() Collection[K, V, Elem]

	// AsCollection returns the collection as a Collection[K, V, Elem].
	AsCollection() Collection[K, V, Elem]
}
