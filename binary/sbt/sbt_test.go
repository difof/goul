package sbt

import (
	"math/rand"
	"testing"
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
	b, err := Load[TestRow]("test.sbt", 1)

	if err != nil {
		t.Fatalf("failed to open SBT file: %v", err)
	}

	t.Logf("file size: %v bytes | num rows: %v", b.Size(), b.NumRows())

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close SBT file: %v", err)
	}
}

func TestBulkAppend(t *testing.T) {
	b, err := Load[*TestRow]("test.sbt", 1)

	if err != nil {
		t.Fatalf("failed to open SBT file: %v", err)
	}

	sz := 1_000_000_000
	flushsz := 10_000
	total := 0
	ri := 0
	rows := make([]*TestRow, flushsz)
	for i := 0; i < sz; i++ {
		rows[ri] = &TestRow{
			Symbol: "BTCUSDT",
			Price:  rand.Uint32(),
		}

		if flushsz == ri+1 {
			if err = b.BulkAppend(rows); err != nil {
				t.Fatalf("failed to bulk append: %v", err)
			}
			total += ri
			ri = 0
		} else {
			ri++
		}
	}

	t.Logf("file size: %v bytes | num rows: %v (%v)", b.Size(), b.NumRows(), total)

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close SBT file: %v", err)
	}
}
