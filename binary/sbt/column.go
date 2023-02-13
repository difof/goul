package sbt

type ColumnType string

const (
	ColumnTypeString  ColumnType = "str"
	ColumnTypeBinary  ColumnType = "bin"
	ColumnTypeBool    ColumnType = "bool"
	ColumnTypeInt8    ColumnType = "i8"
	ColumnTypeInt16   ColumnType = "i16"
	ColumnTypeInt32   ColumnType = "i32"
	ColumnTypeInt64   ColumnType = "i64"
	ColumnTypeUInt8   ColumnType = "u8"
	ColumnTypeUInt16  ColumnType = "u16"
	ColumnTypeUInt32  ColumnType = "u32"
	ColumnTypeUInt64  ColumnType = "u64"
	ColumnTypeFloat32 ColumnType = "f32"
	ColumnTypeFloat64 ColumnType = "f64"
)

func (c ColumnType) New(name string, size ...uint8) Column {
	return NewColumn(name, c, size...)
}

type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
	Size uint8      `json:"size"`
}

// NewColumn creates a new column.
//
// If size is not specified, it will be calculated based on the type.
func NewColumn(name string, typ ColumnType, size ...uint8) (c Column) {
	c = Column{
		Name: name,
		Type: typ,
	}

	if len(size) > 0 {
		c.Size = size[0]
	} else {
		switch typ {
		case ColumnTypeString:
			c.Size = 8
		case ColumnTypeBinary:
			c.Size = 64
		case ColumnTypeBool:
			c.Size = 1
		case ColumnTypeInt8:
			c.Size = 1
		case ColumnTypeInt16:
			c.Size = 2
		case ColumnTypeInt32:
			c.Size = 4
		case ColumnTypeInt64:
			c.Size = 8
		case ColumnTypeUInt8:
			c.Size = 1
		case ColumnTypeUInt16:
			c.Size = 2
		case ColumnTypeUInt32:
			c.Size = 4
		case ColumnTypeUInt64:
			c.Size = 8
		case ColumnTypeFloat32:
			c.Size = 4
		case ColumnTypeFloat64:
			c.Size = 8
		}
	}

	return
}
