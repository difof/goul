package multi_container

import (
	"log"
)

type MultiContainerOptions struct {
	accessArchive   bool
	logger          *log.Logger
	archiveDelaySec int
	onError         func(error)
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

func WithMultiContainerArchiveScheduler(delaySec int) MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.archiveDelaySec = delaySec
	}
}

func WithMultiContainerArchiveAccess() MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.accessArchive = true
	}
}
