//

package ticker

import (
	"github.com/gofrs/uuid"
	"log"
	"time"
)

type TaskHandler func(*Task) error
type TaskErrorHandler func(error, *Task)

const (
	unitMilliseconds = time.Millisecond
	unitSeconds      = time.Second
	unitMinutes      = time.Minute
	unitHours        = time.Hour
	unitDays         = unitHours * 24
	unitWeeks        = unitDays * 7
)

type TaskConfig struct {
	id           uuid.UUID
	oneShot      bool
	nextStep     time.Time // is the nextStep time for this task to run
	lastRun      time.Time
	unit         time.Duration // unit of interval (hours, days or what)
	interval     time.Duration // number of units to repeat (every 3 seconds, the 3 is interval)
	weekDay      time.Weekday
	hour, minute int
	from, to     time.Time

	task          *Task
	scheduler     *Scheduler
	onTick        TaskHandler
	onFinish      TaskHandler
	onBeforeStart TaskHandler
	onError       TaskErrorHandler
}

func (tc *TaskConfig) callHandler(t *Task, f TaskHandler) {
	if f != nil {
		if err := f(t); err != nil {
			if tc.onError != nil {
				tc.onError(err, t)
			} else {
				log.Printf("task error: %s", err)
			}
		}
	}
}

// Do start the task with the supplied payload in a new goroutine.
func (tc *TaskConfig) Do(f TaskHandler, payload ...interface{}) *TaskConfig {
	var thePayload interface{}
	if payload != nil && len(payload) > 0 {
		thePayload = payload[0]
	}

	tc.task = &Task{
		config:  tc,
		Payload: thePayload,
	}

	tc.onTick = f

	tc.scheduler.preRunTask(tc)

	return tc
}

// OnError sets the error handler for the task.
// Errors happening in tasks won't stop the task from running. Call Task.Stop to stop the task.
func (tc *TaskConfig) OnError(f TaskErrorHandler) *TaskConfig {
	tc.onError = f
	return tc
}

// OnFinish sets the finish handler for the task.
func (tc *TaskConfig) OnFinish(f TaskHandler) *TaskConfig {
	tc.onFinish = f
	return tc
}

// OnBeforeStart sets the handler to be called before the task starts.
func (tc *TaskConfig) OnBeforeStart(f TaskHandler) *TaskConfig {
	tc.onBeforeStart = f
	return tc
}

// At sets the time of day to run the task.
func (tc *TaskConfig) At(hour, minute int) *TaskConfig {
	tc.hour = hour
	tc.minute = minute
	return tc
}

// From sets the start time of the task.
func (tc *TaskConfig) From(from time.Time) *TaskConfig {
	tc.from = from
	return tc
}

// To sets the end time of the task.
func (tc *TaskConfig) To(to time.Time) *TaskConfig {
	tc.to = to
	return tc
}

// After starts the task after the specified duration. Short hand for From(time.Now().Add(interval))
func (tc *TaskConfig) After(interval time.Duration) *TaskConfig {
	return tc.From(time.Now().Add(interval))
}

// Millisecond sets the interval to milliseconds.
func (tc *TaskConfig) Millisecond() *TaskConfig { return tc.Milliseconds() }

// Milliseconds is same as Millisecond.
func (tc *TaskConfig) Milliseconds() *TaskConfig {
	tc.unit = unitMilliseconds
	return tc
}

// Second sets the interval to seconds.
func (tc *TaskConfig) Second() *TaskConfig { return tc.Seconds() }

// Seconds is same as Second.
func (tc *TaskConfig) Seconds() *TaskConfig {
	tc.unit = unitSeconds
	return tc
}

// Minute sets the interval to minutes.
func (tc *TaskConfig) Minute() *TaskConfig { return tc.Minutes() }

// Minutes is same as Minute.
func (tc *TaskConfig) Minutes() *TaskConfig {
	tc.unit = unitMinutes
	return tc
}

// Hour sets the interval to hours.
func (tc *TaskConfig) Hour() *TaskConfig { return tc.Hours() }

// Hours is same as Hour.
func (tc *TaskConfig) Hours() *TaskConfig {
	tc.unit = unitHours
	return tc
}

// Day sets the interval to days.
func (tc *TaskConfig) Day() *TaskConfig { return tc.Days() }

// Days is same as Day.
func (tc *TaskConfig) Days() *TaskConfig {
	tc.unit = unitDays
	return tc
}

// Week sets the interval to weeks.
func (tc *TaskConfig) Week() *TaskConfig { return tc.Weeks() }

// Weeks is same as Week.
func (tc *TaskConfig) Weeks() *TaskConfig {
	tc.unit = unitWeeks
	return tc
}

// Saturday sets the unit to weeks and only runs on Saturdays.
func (tc *TaskConfig) Saturday() *TaskConfig {
	tc.unit = unitWeeks
	tc.weekDay = time.Saturday
	return tc
}

// Sunday sets the unit to weeks and only runs on Sundays.
func (tc *TaskConfig) Sunday() *TaskConfig {
	tc.unit = unitWeeks
	tc.weekDay = time.Sunday
	return tc
}

// Monday sets the unit to weeks and only runs on Mondays.
func (tc *TaskConfig) Monday() *TaskConfig {
	tc.unit = unitWeeks
	tc.weekDay = time.Monday
	return tc
}

// Tuesday sets the unit to weeks and only runs on Tuesdays.
func (tc *TaskConfig) Tuesday() *TaskConfig {
	tc.unit = unitWeeks
	tc.weekDay = time.Tuesday
	return tc
}

// Wednesday sets the unit to weeks and only runs on Wednesdays.
func (tc *TaskConfig) Wednesday() *TaskConfig {
	tc.unit = unitWeeks
	tc.weekDay = time.Wednesday
	return tc
}

// Thursday sets the unit to weeks and only runs on Thursdays.
func (tc *TaskConfig) Thursday() *TaskConfig {
	tc.unit = unitWeeks
	tc.weekDay = time.Thursday
	return tc
}

// Friday sets the unit to weeks and only runs on Fridays.
func (tc *TaskConfig) Friday() *TaskConfig {
	tc.unit = unitWeeks
	tc.weekDay = time.Friday
	return tc
}
