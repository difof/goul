package biof

import (
	"encoding/binary"
	"testing"
)

type BIOFTestRow struct {
	Symbol string
	Price  uint32
}

func (h *BIOFTestRow) Factory() Row {
	return new(BIOFTestRow)
}

func (h *BIOFTestRow) Columns() RowSpec {
	return NewRowSpec(
		NewColumn("Symbol", "ascii", 8),
		NewColumn("Price", "uint32", 4),
	)
}

func (h *BIOFTestRow) Encode(buf []byte) error {
	copy(buf[:8], StringToBytePadded(h.Symbol, 8))
	binary.LittleEndian.PutUint32(buf[8:], h.Price)

	return nil
}

func (h *BIOFTestRow) Decode(data []byte) error {
	h.Symbol = ByteToStringPadded(data[:8])
	h.Price = binary.LittleEndian.Uint32(data[8:])

	return nil
}

func TestCreate(t *testing.T) {
	b, err := Create("test.biof", 1, new(BIOFTestRow).Columns())

	if err != nil {
		t.Fatalf("failed to create BIOF file: %v", err)
	}

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close BIOF file: %v", err)
	}
}

func TestOpen(t *testing.T) {
	b, err := Open("test.biof", 1)

	if err != nil {
		t.Fatalf("failed to open BIOF file: %v", err)
	}

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close BIOF file: %v", err)
	}
}

func TestBulkAppend(t *testing.T) {
	b, err := Open[*BIOFTestRow]("test.biof", 1)

	if err != nil {
		t.Fatalf("failed to open BIOF file: %v", err)
	}

	rows := []*BIOFTestRow{}
	for i := 0; i < 10000000; i++ {
		rows = append(rows, &BIOFTestRow{
			Symbol: "MSFT",
			Price:  uint32(i),
		})
	}

	if err = b.BulkAppend(rows); err != nil {
		t.Fatalf("failed to bulk append: %v", err)
	}

	// read row 1
	row := new(BIOFTestRow)

	if err = b.ReadAt(row, 100); err != nil {
		t.Fatalf("failed to read row: %v", err)
	}

	if row.Price != 100 {
		t.Fatalf("invalid symbol: %v", row.Symbol)
	}

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close BIOF file: %v", err)
	}
}
