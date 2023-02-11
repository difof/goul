package sbt

type BIOFColumnType string

const (
	ColumnTypeString BIOFColumnType = "str"
	ColumnTypeBinary BIOFColumnType = "bin"
	ColumnTypeBool   BIOFColumnType = "bool"
	ColumnTypeInt8   BIOFColumnType = "i8"
	ColumnTypeInt16  BIOFColumnType = "i16"
	ColumnTypeInt32  BIOFColumnType = "i32"
	ColumnTypeInt64  BIOFColumnType = "i64"
	ColumnTypeUInt8  BIOFColumnType = "u8"
	ColumnTypeUInt16 BIOFColumnType = "u16"
	ColumnTypeUInt32 BIOFColumnType = "u32"
	ColumnTypeUInt64 BIOFColumnType = "u64"
	ColumnTypeFloat  BIOFColumnType = "f32"
	ColumnTypeDouble BIOFColumnType = "f64"
)

type Column struct {
	Name string         `json:"name"`
	Type BIOFColumnType `json:"type"`
	Size uint8          `json:"size"`
}

// NewColumn creates a new column.
//
// If size is not specified, it will be calculated based on the type.
func NewColumn(name string, typ BIOFColumnType, size ...uint8) (c Column) {
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
		case ColumnTypeFloat:
			c.Size = 4
		case ColumnTypeDouble:
			c.Size = 8
		}
	}

	return
}
