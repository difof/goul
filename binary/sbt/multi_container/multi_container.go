package multi_container

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/difof/goul/binary/sbt"
	"github.com/difof/goul/generics"
	"github.com/difof/goul/generics/containers"
	"github.com/difof/goul/task"
)

type MultiContainerIteratorKey struct {
	Filename string
	RowId    int64
}

var ErrNoFileFound = errors.New("no file found")

// Container is a AcquireContainer wrapper allowing data insertion in multiple serial files
// with archive control to save space.
//
// It use a predefined filename format for identifying files.
// The format is <prefix>_<date 2006-01-02-15-04>_<unix time>.sbt
type Container[P generics.Ptr[RowType], RowType any] struct {
	container         *sbt.Container[P, RowType]
	am                *ArchiveManager
	containerMutex    sync.Mutex
	rootDir           string
	prefix            string
	opts              *Options
	archiveTaskRunner *task.TaskRunner
}

// NewContainer creates a new Container and loads the last container file based on the mode
func NewContainer[P generics.Ptr[RowType], RowType any](
	rootDir, prefix string, options ...Option,
) (c *Container[P, RowType], err error) {
	c = &Container[P, RowType]{
		rootDir: rootDir,
		prefix:  prefix,
		opts: &Options{
			compressionPoolSize: runtime.NumCPU() / 4,
		},
	}

	for _, option := range options {
		option(c.opts)
	}

	c.am, err = NewArchiveManager(rootDir, prefix, 100, c.opts.compressionPoolSize)
	if err != nil {
		return
	}

	if err = c.loadContainer(false); err != nil {
		return
	}

	if c.opts.archiveDelaySec > 0 {
		c.archiveTaskRunner, err = task.Every(c.opts.archiveDelaySec).Seconds().Do(c.archiveTask)
		if err != nil {
			return
		}
	}

	return
}

// loadContainer load a new container and close the current one.
func (c *Container[P, RowType]) loadContainer(forceCreate bool) (err error) {
	c.AcquireContainer()
	defer c.ReleaseContainer()

	if c.container != nil {
		if err = c.closeContainer(); err != nil {
			err = fmt.Errorf("Container (%s): failed to close containers: %w", c.prefix, err)
			return
		}
	}

	if !forceCreate {
		// just load the last file, otherwise continue with the new one
		var lastFilename string
		lastFilename, err = c.am.getLastUncompressedFilename()
		if err != nil {
			return
		}

		if lastFilename != "" {
			c.opts.LogPrintf("Container (%s): opening last file %s", c.prefix, lastFilename)

			if c.opts.openRead {
				c.container, err = sbt.OpenRead[P, RowType](lastFilename)
			} else {
				c.container, err = sbt.Open[P, RowType](lastFilename)
			}

			return
		}
	}

	filename := filepath.Join(c.rootDir, NewMultiContainerFilenamePartsFromNow(c.prefix).String())
	c.opts.LogPrintf("Container (%s): creating %s", c.prefix, filename)
	c.container, err = sbt.Create[P, RowType](filename)

	return
}

// archiveTask
func (c *Container[P, RowType]) archiveTask(*task.Task) error {
	if c.container == nil {
		return nil
	}

	currentFilename := c.container.Filename()

	if c.container.NumRows() > 0 {
		if err := c.loadContainer(true); err != nil {
			return err
		}
	} else {
		return nil
	}

	c.am.QueueCompression(filepath.Join(c.rootDir, currentFilename))

	return nil
}

// closeContainer
func (c *Container[P, RowType]) closeContainer() error {
	c.opts.LogPrintf("Container (%s): closing %s with %d rows",
		c.prefix, c.container.Filename(), c.container.NumRows())
	return c.container.Close()
}

// Close closes the current container and the archive scheduler
func (c *Container[P, RowType]) Close() (err error) {
	if c.archiveTaskRunner != nil {
		if err = c.archiveTaskRunner.Close(); err != nil {
			return
		}
	}

	if err = c.am.Close(); err != nil {
		return
	}

	if c.container != nil {
		if err = c.closeContainer(); err != nil {
			return
		}
	}

	return
}

// AcquireContainer returns the current container in a thread safe way.
// MAKE SURE to call ReleaseContainer when done, otherwise the container will be locked forever.
func (c *Container[P, RowType]) AcquireContainer() *sbt.Container[P, RowType] {
	c.containerMutex.Lock()
	return c.container
}

// ReleaseContainer releases the current container.
func (c *Container[P, RowType]) ReleaseContainer() {
	c.containerMutex.Unlock()
}

// IterRange will begin iteration from the first found file and will stop at the last found file
func (c *Container[P, RowType]) IterRange(
	start, end time.Time,
) *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]] {
	return generics.NewIterator[containers.Tuple[*MultiContainerIteratorKey, P]](c, start, end)
}

// Iter will begin iteration from the very first found file
func (c *Container[P, RowType]) Iter() *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]] {
	return generics.NewIterator[containers.Tuple[*MultiContainerIteratorKey, P]](c)
}

func (c *Container[P, RowType]) IterHandler(
	iter *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]],
) {
	go c.iterate(iter)
}

func (c *Container[P, RowType]) AsIterable() generics.Iterable[containers.Tuple[*MultiContainerIteratorKey, P]] {
	return c
}

func (c *Container[P, RowType]) containerIter(
	underlying *sbt.Container[P, RowType],
	mcIter *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]],
) error {
	cit := underlying.Iter()
	defer cit.Close()

	mcik := &MultiContainerIteratorKey{}
	tuple := containers.NewTuple[*MultiContainerIteratorKey, P](mcik, nil)

	for item := range cit.Next() {
		mcik.RowId = item.Key()
		mcik.Filename = underlying.Filename()

		tuple.First = mcik
		tuple.Second = item.Value()

		select {
		case <-mcIter.Done():
			return nil
		case mcIter.NextChannel() <- tuple:
		}
	}

	if cit.Error() != nil {
		return cit.Error()
	}

	return nil
}

func (c *Container[P, RowType]) filenameIter(
	filename string,
	mcIter *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]],
) (err error) {
	var container *sbt.Container[P, RowType]
	container, err = sbt.Load[P, RowType](filename)
	if err != nil {
		return fmt.Errorf("failed to open container %s for iteration: %w", filename, err)
	}
	defer func() {
		err = container.Close()
	}()

	c.opts.LogPrintf("Container (%s): iterating over %s with %d rows",
		c.prefix, filename, container.NumRows())

	return c.containerIter(container, mcIter)
}

// iterate handles the iterator goroutine
func (c *Container[P, RowType]) iterate(
	iter *generics.Iterator[containers.Tuple[*MultiContainerIteratorKey, P]],
) {
	defer func() {
		if r := recover(); r != nil {
			iter.SetError(fmt.Errorf("panic in ArchiveManager filename provider: %v", r))
		}
	}()
	defer iter.IterationDone()

	start, end := time.Time{}, time.Now()
	if iter.Args != nil {
		start = iter.Args[0].(time.Time)
		end = iter.Args[1].(time.Time)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for filename := range c.am.Files(ctx, start, end) {
		select {
		case <-iter.Done():
			return
		default:
		}

		if err := c.filenameIter(filename, iter); err != nil {
			iter.SetError(err)
			return
		}
	}
}
