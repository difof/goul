package sbt

import (
	"errors"
	"fmt"
	"github.com/difof/goul/binary"
	"github.com/difof/goul/generics"
	"github.com/difof/goul/generics/containers"
	"github.com/difof/goul/task"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrNoFileFound = errors.New("no file found")

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

// String returns the filename
func (p MultiContainerFilenameParts) String() string {
	return fmt.Sprintf("%s_%s_%d.sbt", p.prefix, p.date.Format("2006-01-02-15-04"), p.unix)
}

// SplitMultiContainerFilename splits a filename into parts
func SplitMultiContainerFilename(filename string) (parts MultiContainerFilenameParts, err error) {
	sparts := strings.Split(filename, "_")
	if len(sparts) != 3 {
		err = fmt.Errorf("invalid filename format")
		return
	}

	parts.prefix = sparts[0]
	parts.date, err = time.ParseInLocation("2006-01-02-15-04", sparts[1], time.UTC)
	if err != nil {
		err = fmt.Errorf("invalid date format: %w", err)
		return
	}
	parts.unix, err = strconv.ParseInt(strings.TrimSuffix(sparts[2], ".sbt"), 10, 64)

	return
}

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
	log              bool
	archiveScheduler *task.Scheduler
	archiveDelaySec  int
}

type MultiContainerOption func(*MultiContainerOptions)

func WithMultiContainerLog() MultiContainerOption {
	return func(o *MultiContainerOptions) {
		o.log = true
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

// MultiContainer is a Container wrapper allowing data insertion in multiple serial files
// with archive control to save space.
//
// It use a predefined filename format for identifying files.
// The format is <prefix>_<date 2006-01-02-15-04>_<unix time>.sbt
//
// TODO: iterator to iterate over all files even the archived
type MultiContainer[P generics.Ptr[RowType], RowType any] struct {
	container      *Container[P, RowType]
	am             *ArchiveManager
	containerMutex sync.Mutex
	rootDir        string
	prefix         string
	opts           *MultiContainerOptions
	isArchiving    bool
}

// NewMultiContainer creates a new MultiContainer and loads the last container file based on the mode
func NewMultiContainer[P generics.Ptr[RowType], RowType any](
	dir, prefix string, options ...MultiContainerOption,
) (c *MultiContainer[P, RowType], err error) {
	c = &MultiContainer[P, RowType]{
		rootDir: dir,
		prefix:  prefix,
		opts: &MultiContainerOptions{
			mode: MultiContainerModeAppendLatest,
		},
	}

	for _, option := range options {
		option(c.opts)
	}

	if c.opts.mode != MultiContainerModeNone {
		if err = c.loadContainer(false); err != nil {
			return nil, err
		}
	}

	if c.opts.archiveScheduler != nil {
		c.opts.archiveScheduler.Every(c.opts.archiveDelaySec).Seconds().Do(c.archiveTask).
			OnError(func(err error, t *task.Task) {
				log.Printf("MultiContainer (%s): failed to control archive: %s", c.prefix, err)
			})
	}

	return
}

// loadContainer loads the last container file based on the mode
func (c *MultiContainer[P, RowType]) loadContainer(newContainer bool) error {
	if err := c.Close(); err != nil {
		return fmt.Errorf("MultiContainer (%s): failed to close containers: %w", c.prefix, err)
	}

	if newContainer {
		if err := c.createNewContainer(); err != nil {
			return err
		}
		return nil
	}

	lastFile, archived, err := c.getLastFilename()
	if err != nil {
		if err != ErrNoFileFound {
			return err
		} else {
			lastFile = c.getNewFilename()
		}
	}

	if archived {
		if c.opts.accessArchive {
			c.am, err = LoadArchive(lastFile)
			if err != nil {
				return fmt.Errorf("MultiContainer (%s): failed to load archive %s: %w", c.prefix, lastFile, err)
			}
			lastFile = c.am.DecompressedFilename()
		} else {
			lastFile = c.getNewFilename()
		}
	}

	switch c.opts.mode {
	case MultiContainerModeReadLatest:
		if c.opts.log {
			log.Printf("MultiContainer (%s): opening %s with read last mode", c.prefix, lastFile)
		}
		if c.container, err = OpenRead[P, RowType](lastFile); err != nil {
			return err
		}
	case MultiContainerModeAppendLatest:
		if c.opts.log {
			log.Printf("MultiContainer (%s): opening %s with append last mode", c.prefix, lastFile)
		}
		if c.container, err = Load[P, RowType](lastFile); err != nil {
			return err
		}
	case MultiContainerModeCreate:
		if err = c.createNewContainer(); err != nil {
			return err
		}
	}

	return nil
}

// archiveTask
func (c *MultiContainer[P, RowType]) archiveTask(*task.Task) error {
	if c.isArchiving {
		if c.opts.log {
			log.Printf("MultiContainer (%s): skipping archive control, already archiving", c.prefix)
		}
		return nil
	}

	c.isArchiving = true
	defer func() {
		c.isArchiving = false
	}()

	c.Lock()
	if c.container.NumRows() > 0 {
		if err := c.loadContainer(true); err != nil {
			return err
		}
	}
	c.Unlock()

	files, err := c.globPrefixed(".sbt")
	if err != nil {
		return err
	}

	// remove the c.container.Filename() from the list
	if len(files) > 0 && c.container != nil {
		for i, f := range files {
			if f == c.container.Filename() {
				files = append(files[:i], files[i+1:]...)
				break
			}
		}
	}

	return c.archiveFiles(files)
}

// archiveFiles
func (c *MultiContainer[P, RowType]) archiveFiles(files []string) error {
	if len(files) == 0 {
		return nil
	}

	if c.opts.log {
		log.Printf("MultiContainer (%s): archiving files (%d): %v", c.prefix, len(files), files)
	}

	for _, f := range files {
		start := time.Now()
		if err := binary.EasyGzip(f); err != nil {
			return err
		}

		time.Sleep(5 * time.Millisecond)
		if err := os.Remove(f); err != nil {
			return err
		}

		if c.opts.log {
			duration := time.Since(start)
			log.Printf("MultiContainer (%s): archived %s in %dms", c.prefix, f, duration.Milliseconds())
		}
	}

	return nil
}

// getLastFilename
func (c *MultiContainer[P, RowType]) getLastFilename() (filename string, archived bool, err error) {
	var sbtfiles []string
	if sbtfiles, err = c.globPrefixed(".sbt"); err != nil {
		err = fmt.Errorf("getLastFilename error: %w", err)
		return
	}

	if len(sbtfiles) > 0 {
		sort.Strings(sbtfiles)
		filename = sbtfiles[len(sbtfiles)-1]
		return
	}

	var gzfiles []string
	if gzfiles, err = c.globPrefixed(".sbt.gz"); err != nil {
		err = fmt.Errorf("getLastFilename error: %w", err)
		return
	}

	if len(gzfiles) > 0 {
		sort.Strings(gzfiles)
		filename = gzfiles[len(gzfiles)-1]
		archived = true
		return
	}

	err = ErrNoFileFound
	return
}

// globPrefixed asc sorted with prefix and suffix (extension)
func (c *MultiContainer[P, RowType]) globPrefixed(suffix string) (files []string, err error) {
	pattern := c.prefix + "*" + suffix
	if files, err = filepath.Glob(path.Join(c.rootDir, pattern)); err != nil {
		err = fmt.Errorf("globPrefixed %s error: %w", pattern, err)
		return
	}

	sort.Strings(files)

	return
}

// getNewFilename returns the filename for the next container file
func (c *MultiContainer[P, RowType]) getNewFilename() string {
	return path.Join(c.rootDir, NewMultiContainerFilenamePartsFromNow(c.prefix).String())
}

// createNewContainer creates a new container file
func (c *MultiContainer[P, RowType]) createNewContainer() (err error) {
	filename := c.getNewFilename()
	if c.opts.log {
		log.Printf("MultiContainer (%s): opening %s with create mode", c.prefix, filename)
	}

	if c.opts.log {
		log.Printf("MultiContainer (%s): creating new container %s", c.prefix, filename)
	}

	c.container, err = Create[P, RowType](filename)
	return
}

// Close closes the current container and the archive scheduler
func (c *MultiContainer[P, RowType]) Close() (err error) {
	if c.am != nil {
		if err = c.am.Close(); err != nil {
			return
		}
		c.am = nil
	}

	if c.container != nil {
		if err = c.container.Close(); err != nil {
			return
		}
		c.container = nil
	}

	return
}

// Container returns the current container. You should use Lock and Unlock to avoid race conditions.
func (c *MultiContainer[P, RowType]) Container() *Container[P, RowType] {
	return c.container
}

// Lock should be used when accessing the container
func (c *MultiContainer[P, RowType]) Lock() {
	c.containerMutex.Lock()
}

// Unlock should be used when done accessing the container
func (c *MultiContainer[P, RowType]) Unlock() {
	c.containerMutex.Unlock()
}

func (c *MultiContainer[P, RowType]) Iter() *generics.Iterator[containers.Tuple[int, P]] {
	return generics.NewIterator[containers.Tuple[int, P]](c)
}

func (c *MultiContainer[P, RowType]) IterHandler(iter *generics.Iterator[containers.Tuple[int, P]]) {
	go func() {
		// TODO: bucket load
		var r P

		for i := 0; i < int(c.container.NumRows()); i++ {
			r = any(r).(Row).Factory().(P)
			if err := c.container.ReadAt(r, uint64(i)); err != nil {
				return
			}

			select {
			case <-iter.Done():
				return
			case iter.NextChannel() <- containers.NewTuple(i, r):
			}
		}

		iter.IterationDone()
	}()
}

func (c *MultiContainer[P, RowType]) AsIterable() generics.Iterable[containers.Tuple[int, P]] {
	return c
}
