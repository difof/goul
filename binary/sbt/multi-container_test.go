package sbt

import (
	"github.com/difof/goul/task"
	"testing"
	"time"
)

type TestMCRow struct {
	Name  string
	Value uint64
}

// Factory
func (r *TestMCRow) Factory() Row {
	return new(TestMCRow)
}

// Encode
func (r *TestMCRow) Encode(ctx *Encoder) error {
	ctx.EncodeStringPadded(r.Name, 10)
	ctx.EncodeUInt64(r.Value)
	return nil
}

// Decode
func (r *TestMCRow) Decode(ctx *Decoder) error {
	r.Name = ctx.DecodeStringPadded(10)
	r.Value = ctx.DecodeUInt64()
	return nil
}

// Columns
func (r *TestMCRow) Columns() RowSpec {
	return NewRowSpec(
		ColumnTypeString.New("name", 10),
		ColumnTypeUInt64.New("value"),
	)
}

func createRandomMCArchive(t *testing.T) {
	ts := task.NewScheduler(task.DefaultPrecision)
	ts.Start()
	defer ts.Stop()

	mc, err := NewMultiContainer[*TestMCRow, TestMCRow](".", "testmcrow",
		WithMultiContainerLog(),
		WithMultiContainerArchiveAccess(),
		WithMultiContainerArchiveScheduler(ts, 1),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}

	t.Logf("appending N rows to %s", mc.Container().Filename())
	for i := 0; i < 100_000_000; i++ {
		name := "test"
		if i%2 == 0 {
			name = "test2"
		}

		row := &TestMCRow{
			Name:  name,
			Value: uint64(i),
		}

		mc.Lock()
		if err := mc.Container().Append(row); err != nil {
			mc.Unlock()
			t.Fatalf("failed to append row: %v", err)
		}
		mc.Unlock()

		//time.Sleep(1 * time.Millisecond)
	}

	time.Sleep(5 * time.Second)

	return
}

func TestWithMultiContainerArchiveAccess(t *testing.T) {
	createRandomMCArchive(t)

	t.Logf("opening %s for read", "testmcrow_*.sbt")
	mc, err := NewMultiContainer[*TestMCRow, TestMCRow](".", "testmcrow",
		WithMultiContainerLog(),
		WithMultiContainerMode(MultiContainerModeReadLatest),
		WithMultiContainerArchiveAccess(),
	)
	if err != nil {
		t.Fatalf("failed to create multi container: %v", err)
	}

	t.Logf("reading %d rows from %s", mc.Container().NumRows(), mc.Container().Filename())

	mc.Lock()
	defer mc.Unlock()

	it := mc.Container().Iter()
	defer it.Close()
	for item := range it.Next() {
		row := item.Value()
		t.Logf("row: %+v", row)
	}
}
