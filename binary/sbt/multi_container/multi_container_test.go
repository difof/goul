package multi_container

import (
	"github.com/difof/goul/binary/sbt"
	"log"
	"testing"
	"time"
)

type TestMCRow struct {
	Name  string
	Value uint64
}

// Factory
func (r *TestMCRow) Factory() sbt.Row {
	return new(TestMCRow)
}

// Encode
func (r *TestMCRow) Encode(ctx *sbt.Encoder) error {
	ctx.EncodeStringPadded(r.Name, 10)
	ctx.EncodeUInt64(r.Value)
	return nil
}

// Decode
func (r *TestMCRow) Decode(ctx *sbt.Decoder) error {
	r.Name = ctx.DecodeStringPadded(10)
	r.Value = ctx.DecodeUInt64()
	return nil
}

// Columns
func (r *TestMCRow) Columns() sbt.RowSpec {
	return sbt.NewRowSpec(
		sbt.ColumnTypeString.New("name", 10),
		sbt.ColumnTypeUInt64.New("value"),
	)
}

const largeSize = 100_000_000

func createRandomMCArchive(t *testing.T, archiveDelaySec int) {
	mc, err := NewMultiContainer[*TestMCRow, TestMCRow]("mctest", "test-bybit-linear",
		WithLog(log.Default()),
		WithCompressionScheduler(archiveDelaySec),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}

	defer func() {
		if err := mc.Close(); err != nil {
			t.Fatalf("failed to close multi container: %v", err)
		}
	}()

	appender := sbt.NewBulkAppendContext[*TestMCRow, TestMCRow](sbt.Bucket1k)
	defer func() {
		defer mc.ReleaseContainer()
		if err := appender.Close(mc.AcquireContainer()); err != nil {
			t.Fatalf("failed to close appender: %v", err)
		}
	}()

	var name string
	for i := 0; i < largeSize; i++ {
		name = "test"
		if i%2 == 0 {
			name = "test2"
		}

		if err := appender.Append(mc.AcquireContainer(), &TestMCRow{
			Name:  name,
			Value: uint64(i),
		}); err != nil {
			mc.ReleaseContainer()
			t.Fatalf("failed to append row: %v", err)
		}
		mc.ReleaseContainer()
	}

	return
}

func TestCreate(t *testing.T) {
	createRandomMCArchive(t, 1)
}

func TestWithMultiContainerArchiveAccess(t *testing.T) {
	mc, err := NewMultiContainer[*TestMCRow, TestMCRow]("mctest", "test-bybit-linear",
		WithLog(log.Default()),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}
	defer func() {
		if err := mc.Close(); err != nil {
			t.Fatalf("failed to close multi container: %v", err)
		}
	}()

	it := mc.Iter()
	defer it.Close()

	nread := uint64(0)
	start := time.Now()
	filename := ""

	for item := range it.Next() {
		if filename != item.Key().Filename {
			filename = item.Key().Filename
			t.Logf("reading file %s", filename)
		}

		nread++
	}
	elapsed := time.Since(start)

	if it.Error() != nil {
		t.Fatalf("failed to iterate: %v", it.Error())
	}

	if nread == 0 {
		t.Fatal("no rows read")
	}

	if nread != largeSize {
		t.Fatalf("expected %d rows, got %d", largeSize, nread)
	}

	t.Logf("read %d rows in %dms (%d rows/s total iter speed)",
		nread,
		elapsed.Milliseconds(),
		nread/uint64(elapsed.Seconds()),
	)
}
