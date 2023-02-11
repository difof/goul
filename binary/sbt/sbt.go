package sbt

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	binary2 "github.com/difof/goul/binary"
	"github.com/difof/goul/generics"
	"github.com/difof/goul/generics/containers"
	"github.com/jedib0t/go-pretty/v6/table"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const MagicNumber uint16 = 0xB10F

func rowTypeToInterface[P generics.Ptr[RowType], RowType any](row P) (Row, error) {
	r, ok := any(row).(Row)
	if !ok {
		return nil, fmt.Errorf("row does not implement Row interface")
	}

	if row == nil {
		return r.Factory(), nil
	}

	return r, nil
}

func instanceOfRow[RowType any]() Row {
	var v RowType
	r, ok := any(v).(Row)
	if !ok {
		return nil
	}
	return r.Factory()
}

// Container is a serial binary table data type.
//
// The format contains a header and rows of contents.
//
// Header defines the data types and column formats.
//
// Contents are rows of specified data types with predefined columns and sizes.
//
// It's useful for storing typed streams of data fast and efficiently.
//
// It's not thread-safe.
type Container[P generics.Ptr[RowType], RowType any] struct {
	version    uint8
	headerHash uint64
	spec       RowSpec

	contentOffset uint64
	file          *os.File
	pool          sync.Pool
	filename      string
	numRows       uint64
	headerSize    int32
}

func open[P generics.Ptr[RowType], RowType any](
	filename string,
	version uint8,
	mode int,
	perm os.FileMode,
) (b *Container[P, RowType], err error) {
	b = &Container[P, RowType]{
		filename: filepath.Base(filename),
	}

	// open file
	b.file, err = os.OpenFile(filename, mode, perm)
	if err != nil {
		err = fmt.Errorf("failed to open file: %w", err)
		return
	}

	// read magic number
	var magicNumber uint16
	if err = binary.Read(b.file, binary.LittleEndian, &magicNumber); err != nil {
		err = fmt.Errorf("failed to read magic number: %w", err)
		return
	}

	if magicNumber != MagicNumber {
		err = fmt.Errorf("invalid magic number: %d", magicNumber)
		return
	}

	// read version
	if err = binary.Read(b.file, binary.LittleEndian, &b.version); err != nil {
		err = fmt.Errorf("failed to read version: %w", err)
		return
	}

	if b.version != version {
		err = fmt.Errorf("invalid version: %d", b.version)
		return
	}

	// read header hash
	if err = binary.Read(b.file, binary.LittleEndian, &b.headerHash); err != nil {
		err = fmt.Errorf("failed to read header hash: %w", err)
		return
	}

	// read header size
	if err = binary.Read(b.file, binary.LittleEndian, &b.headerSize); err != nil {
		err = fmt.Errorf("failed to read header size: %w", err)
		return
	}

	// read header
	headerBytes := make([]byte, b.headerSize)
	if _, err = b.file.Read(headerBytes); err != nil {
		err = fmt.Errorf("failed to read header: %w", err)
		return
	}

	// check hash
	if b.headerHash != headerHash(headerBytes) {
		err = fmt.Errorf("invalid header hash %x != %x", b.headerHash, headerHash(headerBytes))
		return
	}

	// unmarshal header
	if err = json.Unmarshal(headerBytes, &b.spec); err != nil {
		err = fmt.Errorf("failed to unmarshal header: %w", err)
		return
	}

	b.contentOffset = uint64(2 + 1 + 8 + 4 + b.headerSize)
	b.pool = binary2.BytePoolN(int(b.spec.RowSize()))
	if b.numRows, err = b.calculateNumRows(); err != nil {
		err = fmt.Errorf("failed to calculate number of rows: %w", err)
		return
	}

	return
}

// OpenRead opens a Container file for reading.
func OpenRead[P generics.Ptr[RowType], RowType any](
	filename string,
	version uint8,
) (b *Container[P, RowType], err error) {
	return open[P, RowType](filename, version, os.O_RDONLY, 0666)
}

// Open opens a Container file.
func Open[P generics.Ptr[RowType], RowType any](
	filename string,
	version uint8,
) (b *Container[P, RowType], err error) {
	return open[P, RowType](filename, version, os.O_RDWR, 0666)
}

// Create creates a Container file.
func Create[P generics.Ptr[RowType], RowType any](
	filename string,
	version uint8,
) (b *Container[P, RowType], err error) {
	ri := instanceOfRow[P]()
	if ri == nil {
		err = fmt.Errorf("failed to create instance of row")
		return
	}

	header := ri.Columns()

	b = &Container[P, RowType]{
		version:  version,
		spec:     header,
		pool:     binary2.BytePoolN(int(header.RowSize())),
		filename: filepath.Base(filename),
	}

	buf := new(bytes.Buffer)

	// write magic number
	if err = binary.Write(buf, binary.LittleEndian, MagicNumber); err != nil {
		err = fmt.Errorf("failed to write magic number: %w", err)
		return
	}

	// write version
	if err = binary.Write(buf, binary.LittleEndian, version); err != nil {
		err = fmt.Errorf("failed to write version: %w", err)
		return
	}

	// header section

	// marshal header
	var headerBytes []byte
	if headerBytes, err = json.Marshal(header); err != nil {
		err = fmt.Errorf("failed to marshal header: %w", err)
		return
	}

	// hash header
	b.headerHash = headerHash(headerBytes)
	b.headerSize = int32(len(headerBytes))

	// write header hash
	if err = binary.Write(buf, binary.LittleEndian, b.headerHash); err != nil {
		err = fmt.Errorf("failed to write header hash: %w", err)
		return
	}

	// write header size
	if err = binary.Write(buf, binary.LittleEndian, uint32(len(headerBytes))); err != nil {
		err = fmt.Errorf("failed to write header size: %w", err)
		return
	}

	// write header
	if _, err = buf.Write(headerBytes); err != nil {
		err = fmt.Errorf("failed to write header: %w", err)
		return
	}

	// open file
	if b.file, err = os.Create(filename); err != nil {
		return
	}

	if _, err = b.file.Write(buf.Bytes()); err != nil {
		err = fmt.Errorf("failed to write header: %w", err)
		return
	}

	b.contentOffset = uint64(buf.Len())
	if b.numRows, err = b.calculateNumRows(); err != nil {
		err = fmt.Errorf("failed to calculate number of rows: %w", err)
		return
	}

	return
}

// Load opens or creates a Container file.
func Load[P generics.Ptr[RowType], RowType any](
	filename string,
	version uint8,
) (b *Container[P, RowType], err error) {
	if _, err = os.Stat(filename); os.IsNotExist(err) {
		return Create[P, RowType](filename, version)
	}

	return Open[P, RowType](filename, version)
}

// Filename returns the filename of the Container file.
func (c *Container[P, RowType]) Filename() string {
	return c.filename
}

// Close closes the Container file.
func (c *Container[P, RowType]) Close() (err error) {
	if c.file != nil {
		err = c.file.Close()
	}

	return
}

// Version returns the version of the Container file.
func (c *Container[P, RowType]) Version() uint8 {
	return c.version
}

// Header returns the header of the Container file.
func (c *Container[P, RowType]) Header() RowSpec {
	return c.spec
}

// SeekContent seeks to the content section of the Container file.
func (c *Container[P, RowType]) SeekContent() (err error) {
	if _, err = c.file.Seek(int64(c.contentOffset), io.SeekStart); err != nil {
		err = fmt.Errorf("failed to seek to content: %w", err)
		return
	}

	return
}

// SeekEnd seeks to the end of the Container file.
func (c *Container[P, RowType]) SeekEnd() (err error) {
	if _, err = c.file.Seek(0, io.SeekEnd); err != nil {
		err = fmt.Errorf("failed to seek to end: %w", err)
		return
	}

	return
}

// calculateNumRows returns the number of rows in the Container file.
func (c *Container[P, RowType]) calculateNumRows() (numRows uint64, err error) {
	var size int64
	if size, err = c.file.Seek(0, io.SeekEnd); err != nil {
		err = fmt.Errorf("failed to seek to end: %w", err)
		return
	}

	numRows = uint64(size-int64(c.contentOffset)) / uint64(c.spec.RowSize())

	return
}

// NumRows returns the number of rows.
func (c *Container[P, RowType]) NumRows() uint64 {
	return c.numRows
}

// Size returns file size
func (c *Container[P, RowType]) Size() uint64 {
	return (c.numRows * uint64(c.spec.RowSize())) + uint64(c.headerSize+1+4+4)
}

// Append appends a row to the Container file.
func (c *Container[P, RowType]) Append(row P) (err error) {
	buf := c.pool.Get().([]byte)
	defer c.pool.Put(buf)

	var r Row
	r, err = rowTypeToInterface(row)
	if err != nil {
		err = fmt.Errorf("failed to convert row to interface: %w", err)
		return
	}

	if err = r.Encode(NewEncoder(buf)); err != nil {
		err = fmt.Errorf("failed to encode row: %w", err)
		return
	}

	if _, err = c.file.Write(buf); err != nil {
		err = fmt.Errorf("failed to write row: %w", err)
		return
	}

	c.numRows++

	return
}

// BulkAppend appends a bulk of rows to the Container file.
func (c *Container[P, RowType]) BulkAppend(rows []P) (err error) {
	buf := new(bytes.Buffer)
	buf.Grow(len(rows) * int(c.spec.RowSize()))

	tmp := c.pool.Get().([]byte)
	defer c.pool.Put(tmp)

	encoder := NewEncoder(nil)

	for _, row := range rows {
		var r Row
		if r, err = rowTypeToInterface(row); err != nil {
			err = fmt.Errorf("failed to convert row to interface: %w", err)
			return
		}

		encoder.Reset(tmp)

		if err = r.Encode(encoder); err != nil {
			err = fmt.Errorf("failed to encode row: %w", err)
			return
		}

		if _, err = buf.Write(tmp); err != nil {
			err = fmt.Errorf("failed to write row: %w", err)
			return
		}
	}

	if _, err = c.file.Write(buf.Bytes()); err != nil {
		err = fmt.Errorf("failed to write rows: %w", err)
		return
	}

	c.numRows += uint64(len(rows))

	return
}

// ReadAt reads a row at a specified position.
func (c *Container[P, RowType]) ReadAt(row P, pos uint64) (err error) {
	var r Row
	if r, err = rowTypeToInterface(row); err != nil {
		err = fmt.Errorf("failed to convert row to interface: %w", err)
		return
	}

	offset := c.contentOffset + pos*uint64(c.spec.RowSize())

	// seek file
	if _, err = c.file.Seek(int64(offset), io.SeekStart); err != nil {
		err = fmt.Errorf("failed to seek file: %w", err)
		return
	}

	// read row
	rowBytes := c.pool.Get().([]byte)
	defer c.pool.Put(rowBytes)

	if _, err = c.file.Read(rowBytes); err != nil {
		err = fmt.Errorf("failed to read row: %w", err)
		return
	}

	// decode row
	if err = r.Decode(NewDecoder(rowBytes)); err != nil {
		err = fmt.Errorf("failed to decode row: %w", err)
		return
	}

	return
}

// BulkRead reads a bulk of rows at a specified position.
//
// rows is a slice of rows to read into. The length of the slice is the number of rows to read.
//
// Use NumRows and pos 0 to read all rows.
//
// returns the number of rows read and an error.
func (c *Container[P, RowType]) BulkRead(rows []P, pos uint64) (n int, err error) {
	rowSize := c.spec.RowSize()
	offset := c.contentOffset + pos*uint64(rowSize)
	byteSize := uint64(int(rowSize) * len(rows))

	if byteSize >= c.Size() {
		byteSize = c.Size() - offset
	}

	// seek file
	if _, err = c.file.Seek(int64(offset), io.SeekStart); err != nil {
		err = fmt.Errorf("failed to seek file: %w", err)
		return
	}

	// read rows
	rowBytes := make([]byte, byteSize)
	if _, err = c.file.Read(rowBytes); err != nil {
		err = fmt.Errorf("failed to read rows: %w", err)
		return
	}

	decoder := NewDecoder(nil)

	// decode rows
	for ; n < len(rows); n++ {
		var r Row
		if r, err = rowTypeToInterface(rows[n]); err != nil {
			err = fmt.Errorf("failed to convert row to interface: %w", err)
			return
		}

		byteOffset := n * int(rowSize)
		byteEnd := byteOffset + int(rowSize)

		if byteEnd > len(rowBytes) {
			break
		}

		decoder.Reset(rowBytes[byteOffset:byteEnd])

		if err = r.Decode(decoder); err != nil {
			err = fmt.Errorf("failed to decode row: %w", err)
			return
		}

		rows[n] = r.(P)
	}

	return
}

type ColumnPrinter[P generics.Ptr[RowType], RowType any] func(row P) []any

func (c *Container[P, RowType]) Print(
	out io.Writer,
	start, count uint64,
	pf ColumnPrinter[P, RowType],
) error {
	rows := make([]P, count)

	readStart := time.Now()
	if _, err := c.BulkRead(rows, start); err != nil {
		return fmt.Errorf("failed to read rows: %v", err)

	}
	readCost := time.Since(readStart)

	t := table.NewWriter()
	t.SetOutputMirror(out)

	header := make(table.Row, len(c.Header())+1)
	header[0] = "#"
	for i, v := range c.Header() {
		header[i+1] = fmt.Sprintf("%s (%s.%d)", v.Name, v.Type, v.Size)
	}
	t.AppendHeader(header)

	trows := make([]table.Row, len(rows))
	for i, v := range rows {
		cp := pf(v)
		trows[i] = make(table.Row, len(cp)+1)
		trows[i][0] = uint64(i) + start
		for j, v := range cp {
			trows[i][j+1] = v
		}
	}

	t.AppendRows(trows)

	t.AppendFooter(table.Row{"", "Total", c.NumRows()})
	t.AppendFooter(table.Row{"", "File size", c.Size()})
	t.AppendFooter(table.Row{"", "Read ms", readCost.Milliseconds()})

	t.Render()

	return nil
}

func (c *Container[P, RowType]) Iter() *generics.Iterator[containers.Tuple[int, P]] {
	return generics.NewIterator[containers.Tuple[int, P]](c)
}

func (c *Container[P, RowType]) IterHandler(iter *generics.Iterator[containers.Tuple[int, P]]) {
	go func() {
		// TODO: bucket load
		var r P

		for i := 0; i < int(c.NumRows()); i++ {
			r = any(r).(Row).Factory().(P)
			if err := c.ReadAt(r, uint64(i)); err != nil {
				return
			}

			select {
			case <-iter.Done():
				return
			case iter.NextChannel() <- containers.NewTuple(i, r):
			}
		}

		iter.IterationDone()
	}()
}

func (c *Container[P, RowType]) AsIterable() generics.Iterable[containers.Tuple[int, P]] {
	return c
}
