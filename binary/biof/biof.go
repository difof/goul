package biof

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	binary2 "github.com/difof/goul/binary"
	"io"
	"os"
	"sync"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Size uint8  `json:"size"`
}

func NewColumn(name string, typ string, size uint8) (c Column) {
	return Column{name, typ, size}
}

type RowSpec []Column

func NewRowSpec(columns ...Column) (r RowSpec) {
	r = make([]Column, len(columns))
	copy(r, columns)

	return
}

// RowSize returns the size of a row.
func (s RowSpec) RowSize() (size uint64) {
	for _, c := range s {
		size += uint64(c.Size)
	}

	return
}

type Row interface {
	Encode([]byte) error
	Decode([]byte) error
	Columns() RowSpec
}

const MagicNumber uint16 = 0xB10F

// BIOF is a Binary IO Format data type.
//
// The format contains a header table and contents.
//
// Header defines the data types and column formats.
//
// Contents are rows of specified data types with predefined columns and sizes.
//
// It's useful for storing typed streams of data fast and efficiently.
//
// It's not thread-safe.
//
// Implements io.Closer.
type BIOF struct {
	version    uint8
	headerHash uint64
	spec       RowSpec

	contentOffset uint64
	file          *os.File
	pool          sync.Pool
}

// Open opens a BIOF file.
func Open(path string, version uint8) (b *BIOF, err error) {
	b = &BIOF{}

	// open file
	b.file, err = os.OpenFile(path, os.O_RDWR, 0644)
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
	var headerSize uint32
	if err = binary.Read(b.file, binary.LittleEndian, &headerSize); err != nil {
		err = fmt.Errorf("failed to read header size: %w", err)
		return
	}

	// read header
	headerBytes := make([]byte, headerSize)
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

	// calculate content offset
	b.contentOffset = uint64(2 + 1 + 8 + 4 + headerSize)

	b.pool = binary2.BytePoolN(int(b.spec.RowSize()))

	return
}

// Create creates a BIOF file.
func Create(path string, version uint8, header RowSpec) (b *BIOF, err error) {
	b = &BIOF{
		version: version,
		spec:    header,
		pool:    binary2.BytePoolN(int(header.RowSize())),
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
	if b.file, err = os.Create(path); err != nil {
		return
	}

	if _, err = b.file.Write(buf.Bytes()); err != nil {
		err = fmt.Errorf("failed to write header: %w", err)
		return
	}

	b.contentOffset = uint64(buf.Len())

	return
}

// Close closes the BIOF file.
func (b *BIOF) Close() (err error) {
	if b.file != nil {
		err = b.file.Close()
	}

	return
}

// Version returns the version of the BIOF file.
func (b *BIOF) Version() uint8 {
	return b.version
}

// Header returns the header of the BIOF file.
func (b *BIOF) Header() RowSpec {
	return b.spec
}

// SeekContent seeks to the content section of the BIOF file.
func (b *BIOF) SeekContent() (err error) {
	if _, err = b.file.Seek(int64(b.contentOffset), io.SeekStart); err != nil {
		err = fmt.Errorf("failed to seek to content: %w", err)
		return
	}

	return
}

// SeekEnd seeks to the end of the BIOF file.
func (b *BIOF) SeekEnd() (err error) {
	if _, err = b.file.Seek(0, io.SeekEnd); err != nil {
		err = fmt.Errorf("failed to seek to end: %w", err)
		return
	}

	return
}

// NumRows returns the number of rows in the BIOF file.
func (b *BIOF) NumRows() (numRows uint64, err error) {
	if err = b.SeekContent(); err != nil {
		return
	}

	var size int64
	if size, err = b.file.Seek(0, io.SeekEnd); err != nil {
		err = fmt.Errorf("failed to seek to end: %w", err)
		return
	}

	numRows = uint64(size-int64(b.contentOffset)) / b.spec.RowSize()

	return
}

// Append appends a row to the BIOF file.
func (b *BIOF) Append(row Row) (err error) {
	buf := b.pool.Get().([]byte)
	defer b.pool.Put(buf)

	if err = row.Encode(buf); err != nil {
		err = fmt.Errorf("failed to encode row: %w", err)
		return
	}

	if _, err = b.file.Write(buf); err != nil {
		err = fmt.Errorf("failed to write row: %w", err)
		return
	}

	return
}

// BulkAppend appends a bulk of rows to the BIOF file.
func (b *BIOF) BulkAppend(rows []Row) (err error) {
	buf := new(bytes.Buffer)
	buf.Grow(len(rows) * int(b.spec.RowSize()))

	tmp := b.pool.Get().([]byte)
	defer b.pool.Put(tmp)

	for _, row := range rows {

		if err = row.Encode(tmp); err != nil {
			err = fmt.Errorf("failed to encode row: %w", err)
			return
		}

		if _, err = buf.Write(tmp); err != nil {
			err = fmt.Errorf("failed to write row: %w", err)
			return
		}
	}

	if _, err = b.file.Write(buf.Bytes()); err != nil {
		err = fmt.Errorf("failed to write rows: %w", err)
		return
	}

	return
}

// ReadAt reads a row at a specified position.
func (b *BIOF) ReadAt(row Row, pos uint64) (err error) {
	rowSize := b.spec.RowSize()
	offset := b.contentOffset + pos*rowSize

	// seek file
	if _, err = b.file.Seek(int64(offset), io.SeekStart); err != nil {
		err = fmt.Errorf("failed to seek file: %w", err)
		return
	}

	// read row
	rowBytes := b.pool.Get().([]byte)
	defer b.pool.Put(rowBytes)

	if _, err = b.file.Read(rowBytes); err != nil {
		err = fmt.Errorf("failed to read row: %w", err)
		return
	}

	// decode row
	if err = row.Decode(rowBytes); err != nil {
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
func (b *BIOF) BulkRead(rows []Row, pos uint64) (n int, err error) {
	rowSize := b.spec.RowSize()
	offset := b.contentOffset + pos*rowSize

	// seek file
	if _, err = b.file.Seek(int64(offset), io.SeekStart); err != nil {
		err = fmt.Errorf("failed to seek file: %w", err)
		return
	}

	// read rows
	// TODO: pool this buffer somehow. it's rough since the size is variable
	rowBytes := make([]byte, int(rowSize)*len(rows))
	if _, err = b.file.Read(rowBytes); err != nil {
		err = fmt.Errorf("failed to read rows: %w", err)
		return
	}

	// decode rows
	for i := 0; i < len(rows); i++ {
		byteOffset := i * int(rowSize)
		byteEnd := byteOffset + int(rowSize)

		if err = rows[i].Decode(rowBytes[byteOffset:byteEnd]); err != nil {
			err = fmt.Errorf("failed to decode row: %w", err)
			return
		}
	}

	return
}
