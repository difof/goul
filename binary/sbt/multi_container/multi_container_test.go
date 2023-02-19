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

func createRandomMCArchive(t *testing.T, archiveDelaySec int) {
	mc, err := NewMultiContainer[*TestMCRow, TestMCRow]("mctest", "testmcrow",
		WithMultiContainerLog(log.Default()),
		WithMultiContainerArchiveAccess(),
		WithMultiContainerArchiveScheduler(archiveDelaySec),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}

	defer func() {
		if err := mc.Close(); err != nil {
			t.Fatalf("failed to close multi container: %v", err)
		}
	}()

	appender := sbt.NewBulkAppendContext[*TestMCRow, TestMCRow](sbt.Bucket10k)
	defer func() {
		defer mc.ReleaseContainer()
		if err := appender.Close(mc.AcquireContainer()); err != nil {
			t.Fatalf("failed to close appender: %v", err)
		}
	}()

	var name string
	for i := 0; i < 100_000_000; i++ {
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
	createRandomMCArchive(t, 2)
}

func TestWithMultiContainerArchiveAccess(t *testing.T) {
	//createRandomMCArchive(t, 1)
	mc, err := NewMultiContainer[*TestMCRow, TestMCRow]("mctest", "testmcrow",
		WithMultiContainerLog(log.Default()),
		WithMultiContainerArchiveAccess(),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}
	defer mc.Close()

	//mc.Lock()
	//defer mc.Unlock()

	it := mc.Iter()
	defer it.Close()

	nread := uint64(0)
	start := time.Now()
	for item := range it.Next() {
		row := item.Value()
		_ = row
		nread++
	}
	elapsed := time.Since(start)

	if it.Error() != nil {
		t.Fatalf("failed to iterate: %v", it.Error())
	}

	t.Logf("read %d rows in %dms (%d rows/s iter speed)",
		nread,
		elapsed.Milliseconds(),
		nread/uint64(elapsed.Seconds()),
	)
}
