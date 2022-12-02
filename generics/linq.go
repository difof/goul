package generics

func SliceMap[T any, U any](slice []T, fn func(T) U) []U {
	result := make([]U, len(slice))

	for i, v := range slice {
		result[i] = fn(v)
	}

	return result
}
