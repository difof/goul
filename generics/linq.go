//

package generics

// SliceMap returns a new slice with the results of applying the given function to each element of the given slice.
func SliceMap[T any, U any](slice []T, fn func(T) U) []U {
	result := make([]U, len(slice))

	for i, v := range slice {
		result[i] = fn(v)
	}

	return result
}

// SliceMapE returns a new slice with the results of applying the given function to each element of the given slice.
func SliceMapE[T any, U any](slice []T, fn func(T) (U, error)) (result []U, err error) {
	result = make([]U, len(slice))

	for i, v := range slice {
		result[i], err = fn(v)
		if err != nil {
			return
		}
	}

	return
}

// SortF sorts the given slice using the given function.
func SortF[T any](slice []T, fn func(T, T) bool) {
	for i := 0; i < len(slice); i++ {
		for j := i + 1; j < len(slice); j++ {
			if fn(slice[i], slice[j]) {
				slice[i], slice[j] = slice[j], slice[i]
			}
		}
	}
}

// SortFE sorts the given slice using the given function.
func SortFE[T any](slice []T, fn func(T, T) (bool, error)) error {
	for i := 0; i < len(slice); i++ {
		for j := i + 1; j < len(slice); j++ {
			if ok, err := fn(slice[i], slice[j]); err != nil {
				return err
			} else if ok {
				slice[i], slice[j] = slice[j], slice[i]
			}
		}
	}

	return nil
}

// Sort sorts the given slice in ascending order.
func Sort[T LTGTConstraint](slice []T) {
	SortF(slice, func(a, b T) bool {
		return a > b
	})
}

// SortReverse sorts the given slice in descending order.
func SortReverse[T LTGTConstraint](slice []T) {
	SortF(slice, func(a, b T) bool {
		return a < b
	})
}
