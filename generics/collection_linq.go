package generics

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
