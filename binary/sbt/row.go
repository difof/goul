package sbt

type RowSpec []Column

func NewRowSpec(columns ...Column) (r RowSpec) {
	r = make([]Column, len(columns))
	copy(r, columns)

	return
}

// RowSize returns the size of a row.
func (s RowSpec) RowSize() (size uint8) {
	for _, c := range s {
		size += c.Size
	}

	return
}

type Row interface {
	Factory() Row
	Encode(ctx *Encoder) error
	Decode(ctx *Decoder) error
	Columns() RowSpec
}
