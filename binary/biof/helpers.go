package biof

import (
	"bytes"
	"encoding/binary"
	binary2 "github.com/difof/goul/binary"
	"hash/fnv"
	"math"
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

// EncodeFloat32 encodes a float32 value to a byte slice.
func EncodeFloat32(f float32) []byte {
	b := bp4.Get().([]byte)
	defer bp4.Put(b)

	binary.LittleEndian.PutUint32(b, math.Float32bits(f))

	return b
}

// DecodeFloat32 decodes a float32 value from a byte slice.
func DecodeFloat32(b []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b))
}

// EncodeFloat64 encodes a float64 value to a byte slice.
func EncodeFloat64(f float64) []byte {
	b := bp8.Get().([]byte)
	defer bp8.Put(b)

	binary.LittleEndian.PutUint64(b, math.Float64bits(f))

	return b
}

// DecodeFloat64 decodes a float64 value from a byte slice.
func DecodeFloat64(b []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b))
}
