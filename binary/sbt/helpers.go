package sbt

import (
	"bytes"
	binary2 "github.com/difof/goul/binary"
	"hash/fnv"
)

var bp4 = binary2.BytePool4()
var bp8 = binary2.BytePool8()

func headerHash(header []byte) (hash uint64) {
	h := fnv.New64a()
	h.Write(header)
	hash = h.Sum64()

	return
}

// StringToBytePadded converts a string to a byte slice with a fixed length.
// If the string is longer than the length, it will be truncated.
// If the string is shorter than the length, it will be padded with NULL.
func StringToBytePadded(s string, length int) []byte {
	b := []byte(s)

	if len(b) > length {
		return b[:length]
	}

	if len(b) < length {
		b = append(b, make([]byte, length-len(b))...)
	}

	return b
}

// ByteToStringPadded converts a null padded byte slice to a string.
func ByteToStringPadded(b []byte) string {
	return string(bytes.TrimRight(b, "\x00"))
}
