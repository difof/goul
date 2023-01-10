package generics

import "sort"

// Any returns true if any item in the collection matches the predicate.
func Any[Elem any](iterable Iterable[Elem], fn func(Elem) (bool, error)) (bool, error) {
	it := iterable.Iter()
	defer it.Close()

	for item := range it.Next() {
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
func All[Elem any](iterable Iterable[Elem], fn func(Elem) (bool, error)) (bool, error) {
	it := iterable.Iter()
	defer it.Close()

	for item := range it.Next() {
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
func Min[V, Elem any](iterable Iterable[Elem], comparator func(V, V) CompareResult) (min Elem, err error) {
	if comp, ok := iterable.(Comparable[V, Elem]); !ok {
		err = ErrNotComparable
		return
	} else {
		first := false

		for item := range iterable.Iter().Next() {
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
func Max[V, Elem any](iterable Iterable[Elem], comparator func(V, V) CompareResult) (max Elem, err error) {
	if comp, ok := iterable.(Comparable[V, Elem]); !ok {
		err = ErrNotComparable
		return
	} else {
		first := false

		for item := range iterable.Iter().Next() {
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
func First[K, V, Elem any](c Collection[K, V, Elem]) (first Elem, err error) {
	it := c.Iter()
	first = <-it.Next()
	it.Close()

	return
}

// Last returns the last item in the collection.
func Last[K, V, Elem any](c Collection[K, V, Elem]) (last Elem, err error) {
	for item := range c.Iter().Next() {
		last = item
	}

	return
}

// OrderBy returns a new collection ordered by the given comparator.
func OrderBy[K, V, Elem any](c Collection[K, V, Elem], comparator func(V, V) CompareResult) (r Collection[K, V, Elem]) {
	values := c.Clone().Values()

	sort.Slice(values, func(i, j int) bool {
		return comparator(values[i], values[j]) == LessThan
	})

	r = c.FactoryFrom(values)

	return
}

// Find returns the first item in the collection that matches the predicate.
func Find[Elem any](iterable Iterable[Elem], fn func(Elem) (bool, error)) (item Elem, err error) {
	it := iterable.Iter()
	defer it.Close()

	for item = range it.Next() {
		var ok bool
		ok, err = fn(item)
		if err != nil {
			return item, err
		}

		if ok {
			return
		}
	}

	err = ErrNotFound

	return
}

// FindLast returns the last item in the collection that matches the predicate.
func FindLast[Elem any](iterable Iterable[Elem], fn func(Elem) (bool, error)) (item Elem, err error) {
	it := iterable.Iter()
	defer it.Close()

	var last Elem
	found := false

	for item = range it.Next() {
		var ok bool
		ok, err = fn(item)
		if err != nil {
			return item, err
		}

		if ok {
			last = item
			found = true
		}
	}

	if !found {
		err = ErrNotFound
	}

	return last, err
}

// Select returns a new collection with the items transformed by the given function.
// The n collection is the mapped collection, and it's preferred to be empty.
func Select[K, V, Elem, NewK, NewV, NewElem any](
	c Collection[K, V, Elem],
	n Collection[NewK, NewV, NewElem],
	fn func(Elem) (NewElem, error)) (Collection[NewK, NewV, NewElem], error) {

	it := c.Iter()
	defer it.Close()

	for item := range it.Next() {
		newItem, err := fn(item)
		if err != nil {
			return n, err
		}

		n.AppendElem(newItem)
	}

	return n, nil
}

// Where returns a new collection with the items that match the predicate.
func Where[K, V, Elem any](c Collection[K, V, Elem], fn func(Elem) (bool, error)) (r Collection[K, V, Elem], err error) {
	it := c.Iter()
	defer it.Close()

	r = c.Factory()

	for item := range it.Next() {
		var ok bool
		ok, err = fn(item)
		if err != nil {
			return
		}

		if ok {
			r.AppendElem(item)
		}
	}

	return
}

// Each iterates over the collection and calls the given function for each item.
func Each[Elem any](iterable Iterable[Elem], fn func(Elem) error) error {
	it := iterable.Iter()
	defer it.Close()

	for item := range it.Next() {
		err := fn(item)
		if err != nil {
			return err
		}
	}

	return nil
}

// Skip returns a new collection with the first n items skipped.
func Skip[K, V any, Elem any](c Collection[K, V, Elem], n int) (r Collection[K, V, Elem]) {
	// TODO
	return
}

// Take returns a new collection with the first n items.
func Take[K, V any, Elem any](c Collection[K, V, Elem], n int) (r Collection[K, V, Elem]) {
	// TODO
	return
}
