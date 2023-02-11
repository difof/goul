package generics

// LTGTConstraint is a constraint that requires the type to implement the < and > operators.
type LTGTConstraint interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | uintptr | float32 | float64
}

type Ptr[T any] interface {
	*T
}
