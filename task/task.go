package task

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
)

// Task is passed to task handlers and contains the payload
type Task struct {
	config  *TaskConfig
	args    []any
	elapsed time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
}

func newTask(config *TaskConfig, args []any) (t *Task) {
	t = &Task{
		config: config,
		args:   args,
	}

	t.ctx, t.cancel = context.WithCancel(context.Background())

	return
}

// Args returns the task arguments.
func (t *Task) Args() []any {
	return t.args
}

// Elapsed returns the time elapsed since the last run.
func (t *Task) Elapsed() time.Duration {
	return t.elapsed
}

// Id returns the task id.
func (t *Task) Id() uuid.UUID {
	return t.config.id
}

// Stop stops the task.
func (t *Task) Stop() {
	t.cancel()
}
