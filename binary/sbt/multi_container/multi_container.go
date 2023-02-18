package multi_container

import (
	"errors"
	"fmt"
	"github.com/difof/goul/binary/sbt"
	"github.com/difof/goul/fs"
	"github.com/difof/goul/generics"
	"github.com/difof/goul/generics/containers"
	"github.com/difof/goul/task"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrNoFileFound = errors.New("no file found")

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

// MultiContainer is a Container wrapper allowing data insertion in multiple serial files
// with archive control to save space.
//
// It use a predefined filename format for identifying files.
// The format is <prefix>_<date 2006-01-02-15-04>_<unix time>.sbt
//
// TODO: iterator to iterate over all files even the archived
type MultiContainer[P generics.Ptr[RowType], RowType any] struct {
	container      *sbt.Container[P, RowType]
	am             *ArchiveManager
	containerMutex sync.Mutex
	rootDir        string
	prefix         string
	opts           *MultiContainerOptions
	isArchiving    bool
	archiveWg      sync.WaitGroup
}

// NewMultiContainer creates a new MultiContainer and loads the last container file based on the mode
func NewMultiContainer[P generics.Ptr[RowType], RowType any](
	rootDir, prefix string, options ...MultiContainerOption,
) (c *MultiContainer[P, RowType], err error) {
	c = &MultiContainer[P, RowType]{
		rootDir: rootDir,
		prefix:  prefix,
		opts: &MultiContainerOptions{
			mode: MultiContainerModeAppendLatest,
			onError: func(err error) {
				panic(err)
			},
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
				c.opts.onError(fmt.Errorf("MultiContainer (%s): failed to archive: %w", c.prefix, err))
			})
	}

	return
}

// loadContainer loads the last container file based on the mode
func (c *MultiContainer[P, RowType]) loadContainer(newContainer bool) error {
	c.Lock()
	defer c.Unlock()

	if err := c.Close(); err != nil {
		return fmt.Errorf("MultiContainer (%s): failed to close containers: %w", c.prefix, err)
	}

	if newContainer {
		if err := c.createNewContainer(); err != nil {
			return err
		}
		return nil
	}

	if c.container != nil {
		return nil
	}

	lastFile, archived, err := c.getLastFilename()
	if err != nil {
		if err != ErrNoFileFound {
			// TODO: return error on read mode
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
		c.opts.LogPrintf("MultiContainer (%s): opening %s with read last mode", c.prefix, lastFile)
		if c.container, err = sbt.OpenRead[P, RowType](lastFile); err != nil {
			return err
		}
	case MultiContainerModeAppendLatest:
		c.opts.LogPrintf("MultiContainer (%s): opening %s with append last mode", c.prefix, lastFile)
		if c.container, err = sbt.Load[P, RowType](lastFile); err != nil {
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
	c.archiveWg.Add(1)
	defer c.archiveWg.Done()

	if c.isArchiving {
		if c.container.NumRows() > 0 {
			if err := c.loadContainer(true); err != nil {
				return err
			}
		}
		return nil
	}

	c.isArchiving = true
	defer func() {
		c.isArchiving = false
	}()

	if c.container.NumRows() > 0 {
		if err := c.loadContainer(true); err != nil {
			return err
		}
	}

	files, err := c.globPrefixed(".sbt")
	if err != nil {
		return err
	}

	files = c.removeCurrentContainer(files)

	return c.archiveFiles(files)
}

// archiveFiles
func (c *MultiContainer[P, RowType]) archiveFiles(files []string) error {
	if len(files) == 0 {
		return nil
	}

	c.opts.LogPrintf("MultiContainer (%s): archiving files (%d): %v", c.prefix, len(files), files)

	for _, f := range files {
		start := time.Now()
		if err := fs.EasyGzip(f); err != nil {
			return err
		}

		time.Sleep(5 * time.Millisecond)
		if err := os.Remove(f); err != nil {
			return err
		}

		duration := time.Since(start)
		c.opts.LogPrintf("MultiContainer (%s): archived %s in %dms", c.prefix, f, duration.Milliseconds())
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

	c.opts.LogPrintf("MultiContainer (%s): opening %s with create mode", c.prefix, filename)

	c.container, err = sbt.Create[P, RowType](filename)
	return
}

// removeCurrentContainer removes the current container file from given slice
func (c *MultiContainer[P, RowType]) removeCurrentContainer(files []string) []string {
	if c.container != nil {
		for i, f := range files {
			if strings.Contains(f, c.container.Filename()) {
				return append(files[:i], files[i+1:]...)
			}
		}
	}

	return files
}

// getIterableFilenames returns all filenames that are iterable. Used by IterHandler.
func (c *MultiContainer[P, RowType]) getIterableFilenames(start, end time.Time) (files []string, err error) {
	allFiles, err := c.globPrefixed(".sbt*")
	if err != nil {
		err = fmt.Errorf("getIterableFilenames glob all error: %w", err)
		return
	}

	allFiles = c.removeCurrentContainer(allFiles)

	// group by filename
	mapped := map[string][]string{}
	for _, f := range allFiles {
		absFilename := f
		switch filepath.Ext(f) {
		case ".sbt":
			absFilename = strings.TrimSuffix(f, ".sbt")
		case ".gz":
			absFilename = strings.TrimSuffix(f, ".sbt.gz")
		case ".decompressed":
			absFilename = strings.TrimSuffix(f, ".sbt.decompressed")
		}

		if mapped[absFilename] == nil {
			mapped[absFilename] = []string{}
		}

		mapped[absFilename] = append(mapped[absFilename], f)
	}

	// grab the unique filenames from mapped
	grabber := func(group []string) string {
		if len(group) == 1 {
			return group[0]
		}

		for _, ext := range []string{".sbt", ".sbt.decompressed", ".sbt.gz"} {
			for _, f := range group {
				if strings.HasSuffix(f, ext) {
					return f
				}
			}
		}

		return ""
	}

	var filenames []string
	for _, v := range mapped {
		filenames = append(filenames, grabber(v))
	}
	sort.Strings(filenames)

	files = make([]string, 0, len(filenames))

	// filter files by start and end
	for _, f := range filenames {
		var parts MultiContainerFilenameParts
		parts, err = SplitMultiContainerFilename(f, time.UTC)
		if err != nil {
			err = fmt.Errorf("getIterableFilenames split filename error: %w", err)
			return
		}

		if parts.Prefix() != c.prefix || parts.Date().After(end) || parts.Date().Before(start) {
			continue
		}

		files = append(files, f)
	}

	return
}

// iterate handles the iterator goroutine
func (c *MultiContainer[P, RowType]) iterate(
	iter *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]],
) {
	defer iter.IterationDone()

	start, end := time.Time{}, time.Now()
	if iter.Args != nil {
		start = iter.Args[0].(time.Time)
		end = iter.Args[1].(time.Time)
	}

	toiterate, err := c.getIterableFilenames(start, end)
	if err != nil {
		iter.SetError(err)
		return
	}

	if len(toiterate) == 0 {
		return
	}

	c.opts.LogPrintf("MultiContainer (%s): iterating over %d files", c.prefix, len(toiterate))

	// TODO: unarchive next file while current is being read

	mcik := &MultiContainerIteratorKey{}
	tuple := containers.NewTuple[*MultiContainerIteratorKey, P](mcik, nil)

	containerIter := func(c *sbt.Container[P, RowType]) {
		cit := c.Iter()
		defer cit.Close()

		for item := range cit.Next() {
			mcik.RowId = item.Key()
			mcik.Filename = c.Filename()
			tuple.Set(mcik, item.Value())

			select {
			case <-iter.Done():
				return
			case iter.NextChannel() <- tuple:
			}
		}
	}

	filenameIter := func(filename string) {
		container, err := sbt.Load[P, RowType](filename)
		if err != nil {
			iter.SetError(fmt.Errorf("failed to open container %s for iteration: %w", filename, err))
			return
		}
		defer container.Close()

		c.opts.LogPrintf("MultiContainer (%s): iterating over %s with %d rows",
			c.prefix, filename, container.NumRows())
		containerIter(container)
	}

	for _, f := range toiterate {
		select {
		case <-iter.Done():
			return
		default:
		}

		filename := f
		if filepath.Ext(f) == ".gz" {
			c.opts.LogPrintf("MultiContainer (%s): uncompressing %s", c.prefix, f)
			filename, err = fs.EasyUnGzip(f)
			if err != nil {
				iter.SetError(fmt.Errorf("failed to decompress %s: %w", f, err))
				return
			}
			c.opts.LogPrintf("MultiContainer (%s): uncompressing %s done", c.prefix, f)
		}

		filenameIter(filename)
	}
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

	// TODO: archive files

	return
}

// WaitArchive waits for the archive scheduler to finish
func (c *MultiContainer[P, RowType]) WaitArchive() {
	if c.isArchiving {
		c.archiveWg.Wait()
	}
}

// Container returns the current container. You should use Lock and Unlock to avoid race conditions.
func (c *MultiContainer[P, RowType]) Container() *sbt.Container[P, RowType] {
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

// RemoveDecompressed removes all decompressed files
func (c *MultiContainer[P, RowType]) RemoveDecompressed() error {
	// TODO:
	return nil
}

// DecompressAll decompresses all archived files
func (c *MultiContainer[P, RowType]) DecompressAll() error {
	// TODO:
	return nil
}

// ArchiveAll archives all unarchived files
func (c *MultiContainer[P, RowType]) ArchiveAll() error {
	// TODO: check if both .sbt and .sbt.gz with same name exist, if so remove .gz and start over
	return nil
}

type MultiContainerIteratorKey struct {
	Filename string
	RowId    uint64
}

// IterRange will begin iteration from the first found file and will stop at the last found file
func (c *MultiContainer[P, RowType]) IterRange(
	start, end time.Time,
) *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]] {
	return generics.NewIterator[containers.Tuple[*MultiContainerIteratorKey, P]](c, start, end)
}

// Iter will begin iteration from the very first found file
func (c *MultiContainer[P, RowType]) Iter() *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]] {
	return generics.NewIterator[containers.Tuple[*MultiContainerIteratorKey, P]](c)
}

func (c *MultiContainer[P, RowType]) IterHandler(
	iter *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]],
) {
	go c.iterate(iter)
}

func (c *MultiContainer[P, RowType]) AsIterable() generics.Iterable[containers.Tuple[*MultiContainerIteratorKey, P]] {
	return c
}
