package task

import (
	"testing"
	"time"
)

func createTask(t *testing.T, customTick func(*Task)) *Runner {
	r, err := Every(1).Second().
		OnBeforeStart(func(task *Task) error {
			t.Logf("task %s - elapsed %d (onBeforeStart)", task.Id(), task.Elapsed().Milliseconds())
			return nil
		}).
		Do(func(task *Task) error {
			t.Logf("task %s - elapsed %d", task.Id(), task.Elapsed().Milliseconds())
			if customTick != nil {
				customTick(task)
			}
			return nil
		})

	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}

	return r
}

func TestEvery(t *testing.T) {
	r := createTask(t, nil)

	time.Sleep(4 * time.Second)
	if err := r.Close(); err != nil {
		t.Fatalf("failed to close runner: %v", err)
	}
}

func TestEvery_Stop(t *testing.T) {
	i := 0
	r := createTask(t, func(task *Task) {
		if i == 2 {
			t.Logf("stopping task %s", task.Id())
			task.Stop()
			return
		}
		i++
	})

	time.Sleep(5 * time.Second)
	if err := r.Close(); err != nil {
		t.Fatalf("failed to close runner: %v", err)
	}
}
