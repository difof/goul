package task

import (
	"context"

	"github.com/difof/goul/concurrency"
)

type JobHandler func(ctx context.Context, arg any) error

type Job struct {
	handler JobHandler
	arg     any
}

type JobQueue struct {
	jobs    chan Job
	ctx     concurrency.CancelContext
	lasterr error
	closer  chan struct{}
}

func NewJobQueue(size int) *JobQueue {
	q := &JobQueue{
		jobs:   make(chan Job, size),
		ctx:    concurrency.NewCancelContext(context.Background()),
		closer: make(chan struct{}),
	}

	go q.runner()

	return q
}

// runner
func (q *JobQueue) runner() {
	for {
		select {
		case <-q.ctx.Done():
			q.closer <- struct{}{}
			return
		case job := <-q.jobs:
			q.lasterr = job.handler(q.ctx, job.arg)
		}
	}
}

// Close closes the queue and waits for all jobs to finish
func (q *JobQueue) Close() error {
	q.ctx.Cancel()
	<-q.closer
	return q.lasterr
}

// Queue adds a new job to the queue
func (q *JobQueue) Queue(job JobHandler, arg ...any) {
	q.jobs <- Job{handler: job, arg: arg}
}
