package errors

func Must[T any](r T, err error) T {
	if err != nil {
		panic(err)
	}

	return r
}

func Ignore[T any](r T, _ error) T {
	return r
}
