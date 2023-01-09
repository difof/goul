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
func First[Elem any](iterable Iterable[Elem]) (first Elem, err error) {
	if sizable, ok := iterable.(Sizable); ok && sizable.IsEmpty() {
		err = ErrNotFound
		return
	}

	first = <-iterable.Iter().Next()

	return
}

// Last returns the last item in the collection.
func Last[Elem any](iterable Iterable[Elem]) (last Elem, err error) {
	if sizable, ok := iterable.(Sizable); ok && sizable.IsEmpty() {
		err = ErrNotFound
		return
	}

	for item := range iterable.Iter().Next() {
		last = item
	}

	return
}

// OrderBy returns a new collection ordered by the given comparator.
func OrderBy[K, V any, Elem any](c Collection[K, V, Elem], comparator func(V, V) CompareResult) (r Collection[K, V, Elem]) {
	r = c.Clone()
	vals := r.Values()

	sort.Slice(vals, func(i, j int) bool {
		return comparator(vals[i], vals[j]) == LessThan
	})

	return
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
