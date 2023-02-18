package multi_container

import (
	"github.com/difof/goul/binary/sbt"
	"github.com/difof/goul/task"
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
	ts := task.NewScheduler(task.DefaultPrecision)
	ts.Start()
	defer ts.Stop()

	mc, err := NewMultiContainer[*TestMCRow, TestMCRow]("mctest", "testmcrow",
		WithMultiContainerLog(log.Default()),
		WithMultiContainerArchiveAccess(),
		WithMultiContainerArchiveScheduler(ts, archiveDelaySec),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}

	t.Logf("appending N rows to %s", mc.Container().Filename())
	appender := sbt.NewBulkAppendContext(sbt.Bucket10k, mc.Container())
	defer func() {
		mc.Lock()
		appender.Close(mc.Container())
		mc.Unlock()
	}()

	var name string
	for i := 0; i < 100_000_000; i++ {
		name = "test"
		if i%2 == 0 {
			name = "test2"
		}

		func() {
			mc.Lock()
			defer mc.Unlock()
			if err := appender.Append(mc.Container(), &TestMCRow{
				Name:  name,
				Value: uint64(i),
			}); err != nil {
				t.Fatalf("failed to append row: %v", err)
			}
		}()

		//time.Sleep(1 * time.Microsecond)
	}

	mc.WaitArchive()

	//time.Sleep(5 * time.Second)

	return
}

func TestCreate(t *testing.T) {
	createRandomMCArchive(t, 1)
}

func TestWithMultiContainerArchiveAccess(t *testing.T) {
	//createRandomMCArchive(t, 1)
	mc, err := NewMultiContainer[*TestMCRow, TestMCRow]("mctest", "testmcrow",
		WithMultiContainerLog(log.Default()),
		WithMultiContainerMode(MultiContainerModeNone),
		WithMultiContainerArchiveAccess(),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}

	mc.Lock()
	defer mc.Unlock()

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
