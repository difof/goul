# SBT

Serial Binary Table is a configurable binary format for storing tabular data.
It is designed to be fast and efficient to read and write,
considering much smaller storage usage compared to CSV or JSON.

## Features

- Fast and efficient to read and write
- Concrete types and static binary format
- Easy to handle
- Easy to iterate over contents
- Split file and auto compression support for huge collections (e.g. orderbook data)
- **WIP** Indexing for fast lookups

## Limitations

- Thread safety
- No support for dynamic types, columns have fixed size
- No support for advanced querying like SQL
- Little endian only (for now)

## Usage

You must define the data type of the table before you can use it, implementing the `Row` interface:
```go
package main

type TestRow struct {
	Symbol string
	Price  uint32
}

func (h *TestRow) Factory() sbt.Row {
	return new(TestRow)
}

func (h *TestRow) Columns() sbt.RowSpec {
	return sbt.NewRowSpec(
		sbt.ColumnTypeString.New("name", 8), // 8 bytes for the fixed size string
		sbt.ColumnTypeUInt32.New("value"),
	)
}

func (h *TestRow) Encode(ctx *sbt.Encoder) error {
	ctx.EncodeStringPadded(h.Symbol, 8)
	ctx.EncodeUInt32(h.Price)

	return nil
}

func (h *TestRow) Decode(ctx *sbt.Decoder) error {
	h.Symbol = ctx.DecodeStringPadded(8)
	h.Price = ctx.DecodeUInt32()

	return nil
}
```

### Single file

To load an SBT file (will be created if it doesn't exist):
```go
b, err := sbt.Load[*TestRow, TestRow]("test.sbt")

if err != nil {
    panic(err)
}

log.Printf("file size: %v bytes | num rows: %v", b.Size(), b.NumRows())

if err = b.Close(); err != nil {
    panic(err)
}
```

You can use any of `sbt.Open`, `sbt.OpenRead`, `sbt.Create` or `sbt.Load` for opening a file based on your need.

To iterate over the contents of the file:
```go
// create a new iterator
it := b.Iter()
// it.Close can be used to stop the iteration early within the loop
defer it.Close()

for item := range it.Next() {
    // item is a tuple[int64, *TestRow]
    // use item.Value() to access the row
}

if err := it.Error(); err != nil {
    panic(err)
}
```

### Multiple files (split file)

You can use the [MultiContainer](./multi-container.go) to store multiple SBT files in a single directory,
all with the same format. This is useful for storing huge collections of data, e.g. orderbook data in multiple
continuous files which can be automatically compressed by the `MultiContainer` on predefined intervals.

```go
mc, err := NewMultiContainer[*TestRow, TestRow](".", "testprefix",
    WithLog(log.Default), // allow logging to stdout
    WithMultiContainerArchiveScheduler(10), // enable file split and compression
)
if err != nil {
    panic(err)
}

defer func() {
    if err := mc.Close(); err != nil {
        panic(err)
    }
}()
```