package sbt

import (
	"errors"
	"github.com/difof/goul/generics"
)

const (
	Bucket10   int64 = 10
	Bucket100  int64 = 100
	Bucket1k   int64 = 1000
	Bucket10k  int64 = 10000
	Bucket100k int64 = 100000
	Bucket1m   int64 = 1000000
	Bucket10m  int64 = 10000000
)

type BulkIO[P generics.Ptr[RT], RT any] interface {
	Close(c *Container[P, RT]) error
}

var ErrClosed = errors.New("use of closed BulkIO")

type BulkAppendContext[P generics.Ptr[RT], RT any] struct {
	bucket []P
	index  int64
	closed bool
}

func NewBulkAppendContext[P generics.Ptr[RT], RT any](bucketSize int64) *BulkAppendContext[P, RT] {
	return &BulkAppendContext[P, RT]{
		bucket: make([]P, bucketSize),
	}
}

// Append will append a row to the bucket.
func (w *BulkAppendContext[P, RT]) Append(c *Container[P, RT], row P) error {
	if w.closed {
		return ErrClosed
	}

	w.bucket[w.index] = row
	w.index++

	if w.index == int64(len(w.bucket)) {
		if err := c.BulkAppend(w.bucket); err != nil {
			return err
		}

		w.index = 0
	}

	return nil
}

// Close will ensure all remaining rows are added.
func (w *BulkAppendContext[P, RT]) Close(c *Container[P, RT]) error {
	if w.closed {
		return ErrClosed
	}

	w.closed = true

	if w.index == 0 {
		return nil
	}

	if err := c.BulkAppend(w.bucket[:w.index]); err != nil {
		return err
	}

	return nil
}
