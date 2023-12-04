package slice

import "github.com/difof/goul/generics"

// Any returns true if any element of the given slice is equal to the given value.
func Any[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}

	return false
}

// AnyF returns true if any element of the given slice satisfies the given function.
func AnyF[T any](slice []T, fn func(T) (bool, error)) (result bool, err error) {
	for _, v := range slice {
		if result, err = fn(v); err != nil {
			return
		} else if result {
			return
		}
	}

	return
}

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

// Reverse reverses the given slice.
func Reverse[T any](slice []T) []T {
	for i, j := 0, len(slice)-1; i < j; i, j = i+1, j-1 {
		slice[i], slice[j] = slice[j], slice[i]
	}

	return slice
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

// Find returns the first element of the given slice for which the given function returns true.
func Find[T any](slice []T, fn func(T) (bool, error)) (result T, ok bool, err error) {
	for _, v := range slice {
		if ok, err = fn(v); err != nil {
			return
		} else if ok {
			result = v
			return
		}
	}

	return
}

// FindIndex returns the index of the first element of the given slice for which the given function returns true.
func FindIndex[T any](slice []T, fn func(T) (bool, error)) (result int, ok bool, err error) {
	for i, v := range slice {
		if ok, err = fn(v); err != nil {
			return
		} else if ok {
			result = i
			return
		}
	}

	return
}

// RemoveF removes the first element of the given slice for which the given function returns true.
func RemoveF[T any](slice []T, fn func(T) (bool, error)) (result []T, ok bool, err error) {
	result = make([]T, 0, len(slice))

	for _, v := range slice {
		if ok, err = fn(v); err != nil {
			return
		} else if !ok {
			result = append(result, v)
		}
	}

	return
}

// Remove removes the element at the given index from the given slice.
func Remove[T any](slice []T, index int) []T {
	return append(slice[:index], slice[index+1:]...)
}

// FastRemove removes the element at the given index from the given slice. The order of the elements is not preserved.
// More efficient than Remove.
func FastRemove[T any](slice []T, index int) []T {
	slice[index] = slice[len(slice)-1]
	return slice[:len(slice)-1]
}

// Clone returns a new slice with the same elements as the given slice.
func Clone[T any](slice []T) []T {
	result := make([]T, len(slice))
	copy(result, slice)
	return result
}
