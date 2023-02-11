package biof

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
