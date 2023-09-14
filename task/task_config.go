package task

import (
	"context"
	"time"

	"github.com/difof/goul/concurrency"
	"github.com/gofrs/uuid"
)

type Handler func(*Task) error

const (
	unitMilliseconds = time.Millisecond
	unitSeconds      = time.Second
	unitMinutes      = time.Minute
	unitHours        = time.Hour
	unitDays         = unitHours * 24
	unitWeeks        = unitDays * 7
)

// Every begins configuring a task. supply zero or one intervals. no intervals will be counted as 1
func Every(interval ...int) *TaskConfig {
	i := 1
	if len(interval) > 0 {
		i = interval[0]
	}

	return newConfig(i, false)
}

// Once begins configuring a task. sets the task to run only once. use Config.After or TaskConfig.From to set the time.
func Once() *TaskConfig {
	return newConfig(1, true)
}

// TaskConfig is responsible for configuring a task and scheduling it.
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

	sem           *concurrency.Semaphore
	runner        *TaskRunner
	task          *Task
	onTick        Handler
	onFinish      Handler
	onBeforeStart Handler
}

func newConfig(interval int, oneShot bool) (config *TaskConfig) {
	now := time.Now()

	config = &TaskConfig{
		id:       uuid.Must(uuid.NewV4()),
		lastRun:  now,
		interval: time.Duration(interval),
		weekDay:  now.Weekday(),
		hour:     now.Hour(),
		minute:   now.Minute(),
		oneShot:  oneShot,
		sem:      concurrency.NewSemaphore(1),
	}

	return
}

func (c *TaskConfig) callHandler(f Handler) error {
	if f != nil {
		if err := f(c.task); err != nil {
			return err
		}
	}

	return nil
}

// Do run the task with the supplied payload in a new goroutine.
func (c *TaskConfig) Do(f Handler, args ...any) (r *TaskRunner, err error) {
	c.task = newTask(c, args)
	c.onTick = f

	r, err = newRunner(context.Background(), c)
	c.runner = r

	return
}

// DoContext run the task with the supplied payload in a new goroutine.
func (c *TaskConfig) DoContext(ctx context.Context, f Handler, args ...any) (r *TaskRunner, err error) {
	c.task = newTask(c, args)
	c.onTick = f

	r, err = newRunner(ctx, c)
	c.runner = r

	return
}

// OnFinish sets the finish handler for the task.
func (c *TaskConfig) OnFinish(f Handler) *TaskConfig {
	c.onFinish = f
	return c
}

// OnBeforeStart sets the handler to be called before the task starts.
func (c *TaskConfig) OnBeforeStart(f Handler) *TaskConfig {
	c.onBeforeStart = f
	return c
}

// At sets the time of day to run the task.
func (c *TaskConfig) At(hour, minute int) *TaskConfig {
	c.hour = hour
	c.minute = minute
	return c
}

// From sets the run time of the task.
func (c *TaskConfig) From(from time.Time) *TaskConfig {
	c.from = from
	return c
}

// To sets the end time of the task.
func (c *TaskConfig) To(to time.Time) *TaskConfig {
	c.to = to
	return c
}

// After starts the task after the specified duration. Short hand for From(time.Now().Add(interval))
func (c *TaskConfig) After(interval time.Duration) *TaskConfig {
	return c.From(time.Now().Add(interval))
}

// Millisecond sets the interval to milliseconds.
func (c *TaskConfig) Millisecond() *TaskConfig { return c.Milliseconds() }

// Milliseconds is same as Millisecond.
func (c *TaskConfig) Milliseconds() *TaskConfig {
	c.unit = unitMilliseconds
	return c
}

// Second sets the interval to seconds.
func (c *TaskConfig) Second() *TaskConfig { return c.Seconds() }

// Seconds is same as Second.
func (c *TaskConfig) Seconds() *TaskConfig {
	c.unit = unitSeconds
	return c
}

// Minute sets the interval to minutes.
func (c *TaskConfig) Minute() *TaskConfig { return c.Minutes() }

// Minutes is same as Minute.
func (c *TaskConfig) Minutes() *TaskConfig {
	c.unit = unitMinutes
	return c
}

// Hour sets the interval to hours.
func (c *TaskConfig) Hour() *TaskConfig { return c.Hours() }

// Hours is same as Hour.
func (c *TaskConfig) Hours() *TaskConfig {
	c.unit = unitHours
	return c
}

// Day sets the interval to days.
func (c *TaskConfig) Day() *TaskConfig { return c.Days() }

// Days is same as Day.
func (c *TaskConfig) Days() *TaskConfig {
	c.unit = unitDays
	return c
}

// Week sets the interval to weeks.
func (c *TaskConfig) Week() *TaskConfig { return c.Weeks() }

// Weeks is same as Week.
func (c *TaskConfig) Weeks() *TaskConfig {
	c.unit = unitWeeks
	return c
}

// Saturday sets the unit to weeks and only runs on Saturdays.
func (c *TaskConfig) Saturday() *TaskConfig {
	c.unit = unitWeeks
	c.weekDay = time.Saturday
	return c
}

// Sunday sets the unit to weeks and only runs on Sundays.
func (c *TaskConfig) Sunday() *TaskConfig {
	c.unit = unitWeeks
	c.weekDay = time.Sunday
	return c
}

// Monday sets the unit to weeks and only runs on Mondays.
func (c *TaskConfig) Monday() *TaskConfig {
	c.unit = unitWeeks
	c.weekDay = time.Monday
	return c
}

// Tuesday sets the unit to weeks and only runs on Tuesdays.
func (c *TaskConfig) Tuesday() *TaskConfig {
	c.unit = unitWeeks
	c.weekDay = time.Tuesday
	return c
}

// Wednesday sets the unit to weeks and only runs on Wednesdays.
func (c *TaskConfig) Wednesday() *TaskConfig {
	c.unit = unitWeeks
	c.weekDay = time.Wednesday
	return c
}

// Thursday sets the unit to weeks and only runs on Thursdays.
func (c *TaskConfig) Thursday() *TaskConfig {
	c.unit = unitWeeks
	c.weekDay = time.Thursday
	return c
}

// Friday sets the unit to weeks and only runs on Fridays.
func (c *TaskConfig) Friday() *TaskConfig {
	c.unit = unitWeeks
	c.weekDay = time.Friday
	return c
}
