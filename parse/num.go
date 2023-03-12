package parse

import "strconv"

func MustStringToFloat64(p string) float64 {
	v, err := strconv.ParseFloat(p, 64)
	if err != nil {
		return 0
	}
	return v
}

func MustStringToInt64(p string) int64 {
	v, err := strconv.ParseInt(p, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
