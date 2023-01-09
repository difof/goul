package generics

// SliceMap returns a new slice with the results of applying the given function to each element of the given slice.
func SliceMap[T any, U any](slice []T, fn func(T) (U, error)) (result []U, err error) {
	result = make([]U, len(slice))

	for i, v := range slice {
		if result[i], err = fn(v); err != nil {
			return
		}
	}

	return
}

// SortF sorts the given slice using the given function.
func SortF[T any](slice []T, fn func(T, T) (bool, error)) error {
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

// Sort sorts the given slice in ascending order. The type parameter must be a native number type.
func Sort[T LTGTConstraint](slice []T) {
	SortF(slice, func(a, b T) (bool, error) {
		return a > b, nil
	})
}

// Reverse sorts the given slice in descending order. The type parameter must be a native number type.
func Reverse[T LTGTConstraint](slice []T) {
	SortF(slice, func(a, b T) (bool, error) {
		return a < b, nil
	})
}
