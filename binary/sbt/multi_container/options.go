package multi_container

import (
	"github.com/difof/goul/task"
	"log"
)

type MultiContainerOpenMode int

const (
	MultiContainerModeNone MultiContainerOpenMode = iota
	MultiContainerModeReadLatest
	MultiContainerModeAppendLatest
	MultiContainerModeCreate
)

type MultiContainerOptions struct {
	mode             MultiContainerOpenMode
	accessArchive    bool
	logger           *log.Logger
	archiveScheduler *task.Scheduler
	archiveDelaySec  int
	onError          func(error)
}

// LogPrintf
func (o *MultiContainerOptions) LogPrintf(format string, v ...interface{}) {
	if o.logger != nil {
		o.logger.Printf(format, v...)
	}
}

type MultiContainerOption func(*MultiContainerOptions)

func WithMultiContainerLog(l *log.Logger) MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.logger = l
	}
}

// WithOnError sets the error handler
func WithOnError(onError func(error)) MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.onError = onError
	}
}

func WithMultiContainerArchiveScheduler(s *task.Scheduler, delaySec int) MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.archiveScheduler = s
		o.archiveDelaySec = delaySec
	}
}

func WithMultiContainerMode(mode MultiContainerOpenMode) MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.mode = mode
	}
}

func WithMultiContainerArchiveAccess() MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.accessArchive = true
	}
}
