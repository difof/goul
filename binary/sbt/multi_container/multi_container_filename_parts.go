package multi_container

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type MultiContainerFilenameParts struct {
	prefix string
	date   time.Time
	unix   int64
}

// NewMultiContainerFilenamePartsFromNow creates a new MultiContainerFilenameParts from now
func NewMultiContainerFilenamePartsFromNow(prefix string) MultiContainerFilenameParts {
	return MultiContainerFilenameParts{
		prefix: prefix,
		date:   time.Now().In(time.UTC),
		unix:   time.Now().Unix(),
	}
}

// Prefix
func (p MultiContainerFilenameParts) Prefix() string {
	return p.prefix
}

// Date
func (p MultiContainerFilenameParts) Date() time.Time {
	return p.date
}

// Unix
func (p MultiContainerFilenameParts) Unix() int64 {
	return p.unix
}

// String returns the filename
func (p MultiContainerFilenameParts) String() string {
	return fmt.Sprintf("%s_%s_%d.sbt", p.prefix, p.date.Format("2006-01-02-15-04"), p.unix)
}

// SplitMultiContainerFilename splits a filename into parts
func SplitMultiContainerFilename(filename string, tz *time.Location) (parts MultiContainerFilenameParts, err error) {
	// remove directory
	filename = filepath.Base(filename)
	// remove extension
	filenameParts := strings.SplitN(filename, ".", 2)
	if len(filenameParts) != 2 {
		err = fmt.Errorf("invalid filename format")
		return
	}

	sparts := strings.Split(filenameParts[0], "_")
	if len(sparts) != 3 {
		err = fmt.Errorf("invalid filename format")
		return
	}

	parts.prefix = sparts[0]
	parts.date, err = time.ParseInLocation("2006-01-02-15-04", sparts[1], tz)
	if err != nil {
		err = fmt.Errorf("invalid date format: %w", err)
		return
	}
	parts.unix, err = strconv.ParseInt(sparts[2], 10, 64)

	return
}
