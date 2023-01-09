package generics

type CompareResult int

const (
	LessThan CompareResult = iota
	EqualTo
	GreaterThan
)

type Comparable[V, Iter any] interface {
	Compare(Iter, Iter, func(V, V) CompareResult) CompareResult
}

// NumericComparator is a comparator for numeric types included in LTGTConstraint.
// It's meant to be used as compare handler in Comparable.Compare.
func NumericComparator[V LTGTConstraint](a, b V) CompareResult {
	switch {
	case a == b:
		return EqualTo
	case a < b:
		return LessThan
	default:
		return GreaterThan
	}
}

// StringComparator is a comparator for strings.
// It's meant to be used as compare handler in Comparable.Compare.
func StringComparator(a, b string) CompareResult {
	switch {
	case a == b:
		return EqualTo
	case a < b:
		return LessThan
	default:
		return GreaterThan
	}
}
