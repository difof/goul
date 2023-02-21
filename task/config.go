package task

import (
	"github.com/difof/goul/concurrency"
	"github.com/gofrs/uuid"
	"time"
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
func Every(interval ...int) *Config {
	i := 1
	if len(interval) > 0 {
		i = interval[0]
	}

	return newConfig(i, false)
}

// Once begins configuring a task. sets the task to run only once. use Config.After or TaskConfig.From to set the time.
func Once() *Config {
	return newConfig(1, true)
}

// Config is responsible for configuring a task and scheduling it.
type Config struct {
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
	runner        *Runner
	task          *Task
	onTick        Handler
	onFinish      Handler
	onBeforeStart Handler
}

func newConfig(interval int, oneShot bool) (config *Config) {
	now := time.Now()

	config = &Config{
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

func (c *Config) callHandler(f Handler) error {
	if f != nil {
		if err := f(c.task); err != nil {
			return err
		}
	}

	return nil
}

// Do run the task with the supplied payload in a new goroutine.
func (c *Config) Do(f Handler, args ...any) (r *Runner, err error) {
	c.task = newTask(c, args)
	c.onTick = f

	r, err = newRunner(c)
	c.runner = r

	return
}

// OnFinish sets the finish handler for the task.
func (c *Config) OnFinish(f Handler) *Config {
	c.onFinish = f
	return c
}

// OnBeforeStart sets the handler to be called before the task starts.
func (c *Config) OnBeforeStart(f Handler) *Config {
	c.onBeforeStart = f
	return c
}

// At sets the time of day to run the task.
func (c *Config) At(hour, minute int) *Config {
	c.hour = hour
	c.minute = minute
	return c
}

// From sets the run time of the task.
func (c *Config) From(from time.Time) *Config {
	c.from = from
	return c
}

// To sets the end time of the task.
func (c *Config) To(to time.Time) *Config {
	c.to = to
	return c
}

// After starts the task after the specified duration. Short hand for From(time.Now().Add(interval))
func (c *Config) After(interval time.Duration) *Config {
	return c.From(time.Now().Add(interval))
}

// Millisecond sets the interval to milliseconds.
func (c *Config) Millisecond() *Config { return c.Milliseconds() }

// Milliseconds is same as Millisecond.
func (c *Config) Milliseconds() *Config {
	c.unit = unitMilliseconds
	return c
}

// Second sets the interval to seconds.
func (c *Config) Second() *Config { return c.Seconds() }

// Seconds is same as Second.
func (c *Config) Seconds() *Config {
	c.unit = unitSeconds
	return c
}

// Minute sets the interval to minutes.
func (c *Config) Minute() *Config { return c.Minutes() }

// Minutes is same as Minute.
func (c *Config) Minutes() *Config {
	c.unit = unitMinutes
	return c
}

// Hour sets the interval to hours.
func (c *Config) Hour() *Config { return c.Hours() }

// Hours is same as Hour.
func (c *Config) Hours() *Config {
	c.unit = unitHours
	return c
}

// Day sets the interval to days.
func (c *Config) Day() *Config { return c.Days() }

// Days is same as Day.
func (c *Config) Days() *Config {
	c.unit = unitDays
	return c
}

// Week sets the interval to weeks.
func (c *Config) Week() *Config { return c.Weeks() }

// Weeks is same as Week.
func (c *Config) Weeks() *Config {
	c.unit = unitWeeks
	return c
}

// Saturday sets the unit to weeks and only runs on Saturdays.
func (c *Config) Saturday() *Config {
	c.unit = unitWeeks
	c.weekDay = time.Saturday
	return c
}

// Sunday sets the unit to weeks and only runs on Sundays.
func (c *Config) Sunday() *Config {
	c.unit = unitWeeks
	c.weekDay = time.Sunday
	return c
}

// Monday sets the unit to weeks and only runs on Mondays.
func (c *Config) Monday() *Config {
	c.unit = unitWeeks
	c.weekDay = time.Monday
	return c
}

// Tuesday sets the unit to weeks and only runs on Tuesdays.
func (c *Config) Tuesday() *Config {
	c.unit = unitWeeks
	c.weekDay = time.Tuesday
	return c
}

// Wednesday sets the unit to weeks and only runs on Wednesdays.
func (c *Config) Wednesday() *Config {
	c.unit = unitWeeks
	c.weekDay = time.Wednesday
	return c
}

// Thursday sets the unit to weeks and only runs on Thursdays.
func (c *Config) Thursday() *Config {
	c.unit = unitWeeks
	c.weekDay = time.Thursday
	return c
}

// Friday sets the unit to weeks and only runs on Fridays.
func (c *Config) Friday() *Config {
	c.unit = unitWeeks
	c.weekDay = time.Friday
	return c
}
