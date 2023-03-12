package multi_container

import (
	"log"
)

type Options struct {
	logger              *log.Logger
	archiveDelaySec     int
	compressionPoolSize int
	openRead            bool
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

func WithCompressionScheduler(delaySec int) Option {
	return func(o *Options) {
		o.archiveDelaySec = delaySec
	}
}

// WithCompressionPoolSize sets the size of the compression worker pool.
// Defaults to NumCPU / 4.
func WithCompressionPoolSize(size int) Option {
	return func(o *Options) {
		o.compressionPoolSize = size
	}
}

// WithOpenRead sets the container to open the file for reading.
// Defaults to false.
func WithOpenRead() Option {
	return func(o *Options) {
		o.openRead = true
	}
}
