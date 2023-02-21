package multi_container

import (
	"context"
	"errors"
	"fmt"
	"github.com/difof/goul/binary/sbt"
	"github.com/difof/goul/generics"
	"github.com/difof/goul/generics/containers"
	"github.com/difof/goul/task"
	"path"
	"runtime"
	"sync"
	"time"
)

type MultiContainerIteratorKey struct {
	Filename string
	RowId    int64
}

var ErrNoFileFound = errors.New("no file found")

// MultiContainer is a AcquireContainer wrapper allowing data insertion in multiple serial files
// with archive control to save space.
//
// It use a predefined filename format for identifying files.
// The format is <prefix>_<date 2006-01-02-15-04>_<unix time>.sbt
type MultiContainer[P generics.Ptr[RowType], RowType any] struct {
	container         *sbt.Container[P, RowType]
	am                *ArchiveManager
	containerMutex    sync.Mutex
	rootDir           string
	prefix            string
	opts              *Options
	archiveTaskRunner *task.Runner
}

// NewMultiContainer creates a new MultiContainer and loads the last container file based on the mode
func NewMultiContainer[P generics.Ptr[RowType], RowType any](
	rootDir, prefix string, options ...Option,
) (c *MultiContainer[P, RowType], err error) {
	c = &MultiContainer[P, RowType]{
		rootDir: rootDir,
		prefix:  prefix,
		opts: &Options{
			compressionPoolSize: runtime.NumCPU() / 4,
			onError: func(err error) {
				panic(err)
			},
		},
	}

	for _, option := range options {
		option(c.opts)
	}

	c.am, err = NewArchiveManager(rootDir, prefix, 100, c.opts.compressionPoolSize)
	if err != nil {
		return
	}

	// TODO: load the last decompressed container file or open a new one
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
func (c *MultiContainer[P, RowType]) loadContainer(forceCreate bool) (err error) {
	c.AcquireContainer()
	defer c.ReleaseContainer()

	if !forceCreate {
		// just load the last file, otherwise continue with the new one
		var lastFilename string
		lastFilename, err = c.am.getLastUncompressedFilename()
		if err != nil {
			return
		}

		if lastFilename != "" {
			c.opts.LogPrintf("MultiContainer (%s): opening last file %s", c.prefix, lastFilename)
			c.container, err = sbt.Open[P, RowType](lastFilename)
			return
		}
	}

	if c.container != nil {
		if err = c.closeContainer(); err != nil {
			err = fmt.Errorf("MultiContainer (%s): failed to close containers: %w", c.prefix, err)
			return
		}
	}

	filename := path.Join(c.rootDir, NewMultiContainerFilenamePartsFromNow(c.prefix).String())
	c.opts.LogPrintf("MultiContainer (%s): creating %s", c.prefix, filename)
	c.container, err = sbt.Create[P, RowType](filename)

	return
}

// archiveTask
func (c *MultiContainer[P, RowType]) archiveTask(*task.Task) error {
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

	c.am.QueueCompression(path.Join(c.rootDir, currentFilename))

	return nil
}

// closeContainer
func (c *MultiContainer[P, RowType]) closeContainer() error {
	c.opts.LogPrintf("MultiContainer (%s): closing %s with %d rows",
		c.prefix, c.container.Filename(), c.container.NumRows())
	return c.container.Close()
}

// Close closes the current container and the archive scheduler
func (c *MultiContainer[P, RowType]) Close() (err error) {
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
func (c *MultiContainer[P, RowType]) AcquireContainer() *sbt.Container[P, RowType] {
	c.containerMutex.Lock()
	return c.container
}

// ReleaseContainer releases the current container.
func (c *MultiContainer[P, RowType]) ReleaseContainer() {
	c.containerMutex.Unlock()
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

func (c *MultiContainer[P, RowType]) containerIter(
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
		tuple.Set(mcik, item.Value())

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

func (c *MultiContainer[P, RowType]) filenameIter(
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

	c.opts.LogPrintf("MultiContainer (%s): iterating over %s with %d rows",
		c.prefix, filename, container.NumRows())

	return c.containerIter(container, mcIter)
}

// iterate handles the iterator goroutine
func (c *MultiContainer[P, RowType]) iterate(
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
