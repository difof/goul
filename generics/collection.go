package generics

import "errors"

var ErrNotComparable = errors.New("collection not comparable")
var ErrNotFound = errors.New("item not found")

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
type Collection[K, V any] interface {
	Sizable
	Gettable[K, V]
	Settable[K, V]

	Clone() Collection[K, V]
	Collection() Collection[K, V]
}
