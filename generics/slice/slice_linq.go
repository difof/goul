package slice

import "github.com/difof/goul/generics"

// Map returns a new slice with the results of applying the given function to each element of the given slice.
func Map[TIn any, TOut any](slice []TIn, fn func(TIn) (TOut, error)) (result []TOut, err error) {
	result = make([]TOut, len(slice))

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
func Sort[T generics.LTGTConstraint](slice []T) error {
	return SortF(slice, func(a, b T) (bool, error) {
		return a > b, nil
	})
}

// Reverse sorts the given slice in descending order. The type parameter must be a native number type.
func Reverse[T generics.LTGTConstraint](slice []T) error {
	return SortF(slice, func(a, b T) (bool, error) {
		return a < b, nil
	})
}

// Filter returns a new slice containing only the elements of the given slice for which the given function returns true.
func Filter[T any](slice []T, fn func(T) (bool, error)) (result []T, err error) {
	result = []T{}

	for _, v := range slice {
		var ok bool
		if ok, err = fn(v); err != nil {
			return
		} else if ok {
			result = append(result, v)
		}
	}

	return
}
