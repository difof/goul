package multi_container

import (
	"log"
)

type Options struct {
	logger              *log.Logger
	archiveDelaySec     int
	onError             func(error)
	compressionPoolSize int
}

// LogPrintf
func (o *Options) LogPrintf(format string, v ...interface{}) {
	if o.logger != nil {
		o.logger.Printf(format, v...)
	}
}

type Option func(*Options)

func WithLog(l *log.Logger) Option {
	return func(o *Options) {
		o.logger = l
	}
}

// WithOnError sets the error handler
func WithOnError(onError func(error)) Option {
	return func(o *Options) {
		o.onError = onError
	}
}

func WithCompressionScheduler(delaySec int) Option {
	return func(o *Options) {
		o.archiveDelaySec = delaySec
	}
}

func WithCompressionPoolSize(size int) Option {
	return func(o *Options) {
		o.compressionPoolSize = size
	}
}
