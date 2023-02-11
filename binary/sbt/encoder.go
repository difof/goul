package sbt

import (
	"encoding/binary"
	"math"
)

type RowSerializerBase struct {
	buffer  []byte
	counter int
}

// newRowSerializerBase
func newRowSerializerBase(buffer []byte) RowSerializerBase {
	return RowSerializerBase{buffer: buffer}
}

// Bytes returns the byte slice of the serializer.
func (s *RowSerializerBase) Bytes() []byte {
	return s.buffer
}

// Reset resets the encoder.
func (s *RowSerializerBase) Reset(buffer []byte) {
	s.buffer = buffer
	s.counter = 0
}

// Encoder is passed to Row.Encode as the encoding context and helper.
type Encoder struct {
	RowSerializerBase
}

// NewEncoder
func NewEncoder(buffer []byte) *Encoder {
	return &Encoder{newRowSerializerBase(buffer)}
}

// EncodeStringPadded
func (e *Encoder) EncodeStringPadded(s string, size int) {
	copy(e.buffer[e.counter:], StringToBytePadded(s, size))
	e.counter += size
}

// EncodeBytesPadded
func (e *Encoder) EncodeBytesPadded(b []byte, size int) {
	copy(e.buffer[e.counter:], b)
	e.counter += size
}

// EncodeUInt8
func (e *Encoder) EncodeUInt8(v uint8) {
	e.buffer[e.counter] = v
	e.counter++
}

// EncodeUInt16
func (e *Encoder) EncodeUInt16(v uint16) {
	binary.LittleEndian.PutUint16(e.buffer[e.counter:], v)
	e.counter += 2
}

// EncodeUInt32
func (e *Encoder) EncodeUInt32(v uint32) {
	binary.LittleEndian.PutUint32(e.buffer[e.counter:], v)
	e.counter += 4
}

// EncodeUInt64
func (e *Encoder) EncodeUInt64(v uint64) {
	binary.LittleEndian.PutUint64(e.buffer[e.counter:], v)
	e.counter += 8
}

// EncodeInt8
func (e *Encoder) EncodeInt8(v int8) {
	e.buffer[e.counter] = byte(v)
	e.counter++
}

// EncodeInt16
func (e *Encoder) EncodeInt16(v int16) {
	binary.LittleEndian.PutUint16(e.buffer[e.counter:], uint16(v))
	e.counter += 2
}

// EncodeInt32
func (e *Encoder) EncodeInt32(v int32) {
	binary.LittleEndian.PutUint32(e.buffer[e.counter:], uint32(v))
	e.counter += 4
}

// EncodeInt64
func (e *Encoder) EncodeInt64(v int64) {
	binary.LittleEndian.PutUint64(e.buffer[e.counter:], uint64(v))
	e.counter += 8
}

// EncodeFloat32
func (e *Encoder) EncodeFloat32(v float32) {
	binary.LittleEndian.PutUint32(e.buffer[e.counter:], math.Float32bits(v))
	e.counter += 4
}

// EncodeFloat64
func (e *Encoder) EncodeFloat64(v float64) {
	binary.LittleEndian.PutUint64(e.buffer[e.counter:], math.Float64bits(v))
	e.counter += 8
}

// EncodeBool
func (e *Encoder) EncodeBool(v bool) {
	if v {
		e.buffer[e.counter] = 1
	} else {
		e.buffer[e.counter] = 0
	}
	e.counter++
}

// Decoder is passed to Row.Decode as the encoding context and helper.
type Decoder struct {
	RowSerializerBase
}

// NewDecoder
func NewDecoder(buffer []byte) *Decoder {
	return &Decoder{newRowSerializerBase(buffer)}
}

// DecodeStringPadded
func (d *Decoder) DecodeStringPadded(size int) string {
	s := ByteToStringPadded(d.buffer[d.counter : d.counter+size])
	d.counter += size
	return s
}

// DecodeBytes
func (d *Decoder) DecodeBytes(size int) []byte {
	b := d.buffer[d.counter : d.counter+size]
	d.counter += size
	return b
}

// DecodeUInt8
func (d *Decoder) DecodeUInt8() uint8 {
	v := d.buffer[d.counter]
	d.counter++
	return v
}

// DecodeUInt16
func (d *Decoder) DecodeUInt16() uint16 {
	v := binary.LittleEndian.Uint16(d.buffer[d.counter:])
	d.counter += 2
	return v
}

// DecodeUInt32
func (d *Decoder) DecodeUInt32() uint32 {
	v := binary.LittleEndian.Uint32(d.buffer[d.counter:])
	d.counter += 4
	return v
}

// DecodeUInt64
func (d *Decoder) DecodeUInt64() uint64 {
	v := binary.LittleEndian.Uint64(d.buffer[d.counter:])
	d.counter += 8
	return v
}

// DecodeInt8
func (d *Decoder) DecodeInt8() int8 {
	v := d.buffer[d.counter]
	d.counter++
	return int8(v)
}

// DecodeInt16
func (d *Decoder) DecodeInt16() int16 {
	v := binary.LittleEndian.Uint16(d.buffer[d.counter:])
	d.counter += 2
	return int16(v)
}

// DecodeInt32
func (d *Decoder) DecodeInt32() int32 {
	v := binary.LittleEndian.Uint32(d.buffer[d.counter:])
	d.counter += 4
	return int32(v)
}

// DecodeInt64
func (d *Decoder) DecodeInt64() int64 {
	v := binary.LittleEndian.Uint64(d.buffer[d.counter:])
	d.counter += 8
	return int64(v)
}

// DecodeFloat32
func (d *Decoder) DecodeFloat32() float32 {
	v := binary.LittleEndian.Uint32(d.buffer[d.counter:])
	d.counter += 4
	return math.Float32frombits(v)
}

// DecodeFloat64
func (d *Decoder) DecodeFloat64() float64 {
	v := binary.LittleEndian.Uint64(d.buffer[d.counter:])
	d.counter += 8
	return math.Float64frombits(v)
}

// DecodeBool
func (d *Decoder) DecodeBool() bool {
	v := d.buffer[d.counter]
	d.counter++
	return v != 0
}
