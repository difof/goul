package ticker

import (
	"github.com/difof/goul/generics"
	"github.com/gofrs/uuid"
	"sync"
	"time"
)

const DefaultPrecision = time.Millisecond * 100

// Scheduler is used to create and manage tasks.
//
// Scheduler is thread-safe, and it's safe to add tasks after starting the scheduler.
//
// Example:
//
//	s := NewScheduler(DefaultPrecision)
//	s.Every(1).Seconds().Do(func(t *Task) error { return nil })
//	s.Every(2).Hours().Do(func(t *Task) error { return nil })
//	s.Every().Day().After(1 * time.Hour * 24).Do(func(t *Task) error { return nil })
//	s.Once().After(3 * time.Second).Do(taskFactory("once-after-3s"))
//	s.Start()
type Scheduler struct {
	stopCh      chan struct{}
	taskConfigs *generics.SafeMap[uuid.UUID, *TaskConfig]
	wg          sync.WaitGroup
	localTime   *time.Location
	precision   time.Duration
	running     bool
}

// NewScheduler creates a new scheduler.
//
// Smaller precision may add to CPU load with many taskConfigs running, due to periodical checks.
func NewScheduler(precision time.Duration) *Scheduler {
	return &Scheduler{
		stopCh:      make(chan struct{}, 1),
		taskConfigs: generics.NewSafeMap[uuid.UUID, *TaskConfig](),
		localTime:   time.Local,
		precision:   precision,
	}
}

// Start starts the scheduler.
func (s *Scheduler) Start() {
	if s.running {
		return
	}

	s.running = true

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(s.precision)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for _, tc := range s.taskConfigs.Values() {
					s.wg.Add(1)
					go s.runTask(tc)
				}
			case <-s.stopCh:
				for _, tc := range s.taskConfigs.Values() {
					tc.callHandler(tc.task, tc.onFinish)
				}

				return
			}
		}
	}()
}

// Stop stops the scheduler.
func (s *Scheduler) Stop() {
	if !s.running {
		return
	}

	s.stopCh <- struct{}{}
	s.wg.Wait()
	s.running = false
}

// Every begins configuring a task. supply zero or one intervals. no intervals will be counted as 1
func (s *Scheduler) Every(interval ...int) *TaskConfig {
	i := 1
	if len(interval) > 0 {
		i = interval[0]
	}

	return s.newTask(i, false)
}

// Once begins configuring a task. sets the task to run only once. use TaskConfig.After or TaskConfig.From to set the time.
func (s *Scheduler) Once() *TaskConfig {
	return s.newTask(1, true)
}

func (s *Scheduler) newTask(interval int, oneShot bool) (tc *TaskConfig) {
	now := time.Now()
	tc = &TaskConfig{
		id:        uuid.Must(uuid.NewV4()),
		lastRun:   now,
		interval:  time.Duration(interval),
		weekDay:   now.Weekday(),
		hour:      now.Hour(),
		minute:    now.Minute(),
		oneShot:   oneShot,
		scheduler: s,
	}

	s.taskConfigs.Set(tc.id, tc)

	return
}

func (s *Scheduler) removeTask(id uuid.UUID) {
	s.taskConfigs.Delete(id)
}

func (s *Scheduler) preRunTask(tc *TaskConfig) {
	tc.callHandler(tc.task, tc.onBeforeStart)
	s.calculateTaskNextStep(tc)
}

func (s *Scheduler) finishTask(tc *TaskConfig) {
	tc.callHandler(tc.task, tc.onFinish)
	s.removeTask(tc.id)
}

func (s *Scheduler) calculateTaskNextStep(tc *TaskConfig) {
	if tc.unit == unitWeeks {
		now := time.Now()
		remainingDays := tc.weekDay - now.Weekday()
		if remainingDays <= 0 {
			// schedule for nextStep week
			tc.nextStep = now.AddDate(0, 0, 6-int(now.Weekday())+int(tc.weekDay)+1)
		} else {
			tc.nextStep = now.AddDate(0, 0, int(remainingDays))
		}

		tc.nextStep = time.Date(tc.nextStep.Year(), tc.nextStep.Month(), tc.nextStep.Day(), tc.hour, tc.minute, 0, 0, tc.scheduler.localTime)
		tc.nextStep = tc.nextStep.Add((tc.interval - 1) * tc.unit)
	} else if tc.unit == unitDays {
		tc.nextStep = tc.nextStep.Add(tc.interval * tc.unit)
		tc.nextStep = time.Date(tc.nextStep.Year(), tc.nextStep.Month(), tc.nextStep.Day(), tc.hour, tc.minute, 0, 0, tc.scheduler.localTime)
	} else {
		tc.nextStep = time.Now().Add(tc.interval * tc.unit)
	}
}

func (s *Scheduler) runTask(tc *TaskConfig) {
	defer s.wg.Done()

	if time.Since(tc.nextStep) <= 0 {
		return
	}

	if time.Now().After(tc.from) {
		tc.task.Elapsed = time.Since(tc.lastRun)
		tc.callHandler(tc.task, tc.onTick)
		tc.lastRun = time.Now()

		if tc.oneShot {
			tc.callHandler(tc.task, tc.onFinish)
			s.removeTask(tc.id)
			return
		}
	}

	s.calculateTaskNextStep(tc)

	if tc.to.Year() != 1 && tc.nextStep.After(tc.to) {
		tc.callHandler(tc.task, tc.onFinish)
		s.removeTask(tc.id)
	}
}
