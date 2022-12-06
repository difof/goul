//

package ticker

import (
	"github.com/gofrs/uuid"
	"time"
)

// Task is passed to task handlers and contains the payload
type Task struct {
	config  *TaskConfig
	Payload interface{}
	Elapsed time.Duration
}

// Id returns the task id.
func (t *Task) Id() uuid.UUID {
	return t.config.id
}

// Stop stops the task.
func (t *Task) Stop() {
	t.config.scheduler.removeTask(t.config.id)
}
