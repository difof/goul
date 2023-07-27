package task

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TaskRunner is responsible for running the task given to them
type TaskRunner struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	config *TaskConfig
	err    error
}

func newRunner(config *TaskConfig) (r *TaskRunner, err error) {
	r = &TaskRunner{
		config: config,
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.wg.Add(1)
	go r.run()

	if err = r.config.callHandler(r.config.onBeforeStart); err != nil {
		err = fmt.Errorf("error calling onBeforeStart handler: %w", err)
		return
	}

	r.calculateTaskNextStep()

	return
}

// run starts the task runner.
func (r *TaskRunner) run() {
	tickDelay := unitSeconds / 100
	if r.config.unit == unitMilliseconds {
		tickDelay = unitMilliseconds / 5
	}

	ticker := time.NewTicker(tickDelay)
	defer ticker.Stop()
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.config.task.ctx.Done():
			return
		case <-ticker.C:
			if err := r.handleTask(); err != nil {
				r.err = err
				return
			}
		}
	}
}

// handleTask
func (r *TaskRunner) handleTask() (err error) {
	// do nothing if it's just started
	if time.Since(r.config.nextStep) <= 0 {
		return
	}

	if time.Now().After(r.config.from) {
		r.config.task.elapsed = time.Since(r.config.lastRun)
		if err = r.config.callHandler(r.config.onTick); err != nil {
			return
		}

		r.config.lastRun = time.Now()

		if r.config.oneShot {
			err = r.config.callHandler(r.config.onFinish)
			return
		}
	}

	r.calculateTaskNextStep()

	// finish the task if it's past the end date (if end date is provided)
	if r.config.to.Year() != 1 && r.config.nextStep.After(r.config.to) {
		err = r.config.callHandler(r.config.onFinish)
	}

	return
}

// calculateTaskNextStep calculates the next step of the task.
func (r *TaskRunner) calculateTaskNextStep() {
	if r.config.unit == unitWeeks {
		now := time.Now()
		remainingDays := r.config.weekDay - now.Weekday()
		if remainingDays <= 0 {
			// schedule for nextStep week
			r.config.nextStep = now.AddDate(0, 0, 6-int(now.Weekday())+int(r.config.weekDay)+1)
		} else {
			r.config.nextStep = now.AddDate(0, 0, int(remainingDays))
		}

		r.config.nextStep = time.Date(
			r.config.nextStep.Year(), r.config.nextStep.Month(), r.config.nextStep.Day(),
			r.config.hour, r.config.minute, 0, 0,
			time.Local)

		r.config.nextStep = r.config.nextStep.Add((r.config.interval - 1) * r.config.unit)
	} else if r.config.unit == unitDays {
		r.config.nextStep = r.config.nextStep.Add(r.config.interval * r.config.unit)
		r.config.nextStep = time.Date(
			r.config.nextStep.Year(), r.config.nextStep.Month(), r.config.nextStep.Day(),
			r.config.hour, r.config.minute, 0, 0,
			time.Local)
	} else {
		r.config.nextStep = time.Now().Add(r.config.interval * r.config.unit)
	}
}

// Close closes the task runner.
func (r *TaskRunner) Close() error {
	r.cancel()
	r.wg.Wait()
	return r.err
}
