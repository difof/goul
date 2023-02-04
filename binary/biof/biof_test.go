package biof

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

type BIOFTestRow struct {
	Symbol  string
	PriceE8 uint64
}

func (h *BIOFTestRow) Columns() RowSpec {
	return NewRowSpec(
		NewColumn("Symbol", "ascii", 8),
		NewColumn("PriceE8", "uint64", 8),
	)
}

func (h *BIOFTestRow) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	if _, err := buf.Write(StringToBytePadded(h.Symbol, 8)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, h.PriceE8); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (h *BIOFTestRow) Decode(data []byte) error {
	if len(data) < 16 {
		return errors.New("invalid data length")
	}

	h.Symbol = PaddedByteToString(data[:8])
	h.PriceE8 = binary.LittleEndian.Uint64(data[8:])

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
	b, err := Open("test.biof", 1)

	if err != nil {
		t.Fatalf("failed to open BIOF file: %v", err)
	}

	rows := []Row{
		&BIOFTestRow{
			Symbol:  "AAPL",
			PriceE8: 100000000,
		},
		&BIOFTestRow{
			Symbol:  "MSFT",
			PriceE8: 200000000,
		},
		&BIOFTestRow{
			Symbol:  "GOOG",
			PriceE8: 300000000,
		},
	}

	if err = b.BulkAppend(rows); err != nil {
		t.Fatalf("failed to bulk append: %v", err)
	}

	// read row 1
	row := new(BIOFTestRow)

	if err = b.ReadAt(row, 1); err != nil {
		t.Fatalf("failed to read row: %v", err)
	}

	if row.Symbol != "MSFT" {
		t.Fatalf("invalid symbol: %v", row.Symbol)
	}

	if err = b.Close(); err != nil {
		t.Fatalf("failed to close BIOF file: %v", err)
	}
}
