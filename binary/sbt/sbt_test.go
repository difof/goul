package sbt

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

type TestRow struct {
	Symbol string
	Price  uint32
}

func (h *TestRow) Factory() Row {
	return new(TestRow)
}

func (h *TestRow) Columns() RowSpec {
	return NewRowSpec(
		NewColumn("Symbol", "ascii", 8),
		NewColumn("Price", "uint32", 4),
	)
}

func (h *TestRow) Encode(ctx *Encoder) error {
	ctx.EncodeStringPadded(h.Symbol, 8)
	ctx.EncodeUInt32(h.Price)

	return nil
}

func (h *TestRow) Decode(ctx *Decoder) error {
	h.Symbol = ctx.DecodeStringPadded(8)
	h.Price = ctx.DecodeUInt32()

	return nil
}

func TestLoad(t *testing.T) {
	b, err := Load[*TestRow, TestRow]("test.sbt")

	if err != nil {
		t.Fatalf("failed to open AcquireContainer file: %v", err)
	}

	t.Logf("file size: %v bytes | num rows: %v", b.Size(), b.NumRows())

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close AcquireContainer file: %v", err)
	}
}

const LargeSize = 100_000

func createBigSBT(t *testing.T) {
	b, err := Create[*TestRow, TestRow]("test.sbt")

	if err != nil {
		t.Fatalf("failed to open AcquireContainer file: %v", err)
	}

	sz := LargeSize
	bulk := NewBulkAppendContext[*TestRow, TestRow](Bucket10k)

	start := time.Now()
	for i := 0; i < sz; i++ {
		if err := bulk.Append(b, &TestRow{
			Symbol: "BTCUSDT",
			Price:  rand.Uint32(),
		}); err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	if err := bulk.Close(b); err != nil {
		t.Fatalf("failed to close bulk append context: %v", err)
	}

	t.Logf("file size: %dMB | num rows: %d | append time %dms",
		b.Size()/1024/1024, b.NumRows(), time.Since(start).Milliseconds())

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close AcquireContainer file: %v", err)
	}
}

func TestBulkAppend(t *testing.T) {
	createBigSBT(t)
}

func TestIterate(t *testing.T) {
	b, err := Load[*TestRow, TestRow]("test.sbt")
	if err != nil {
		t.Fatalf("failed to open AcquireContainer file: %v", err)
	}

	it := b.IterBucketSize(35_000)
	defer it.Close()

	start := time.Now()
	n := 0
	for item := range it.Next() {
		row := item.Value()
		_ = row
		n++
	}

	if it.Error() != nil {
		t.Fatalf("failed to iterate: %v", it.Error())
	}

	if n != LargeSize {
		t.Fatalf("expected %d rows, got %d", LargeSize, n)
	}

	t.Logf("Iteration took %dms", time.Since(start).Milliseconds())
}

func TestPrint(t *testing.T) {
	b, err := OpenRead[*TestRow, TestRow]("test.sbt")

	if err != nil {
		t.Fatalf("failed to open AcquireContainer file: %v", err)
	}

	b.Print(os.Stdout, 0, 10, func(row *TestRow) []any {
		return []any{row.Symbol, row.Price}
	})

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close AcquireContainer file: %v", err)
	}
}
