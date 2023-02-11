package sbt

import (
	"fmt"
	"github.com/difof/goul/binary"
	"github.com/difof/goul/generics"
	"github.com/difof/goul/task"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

type MultiContainer[P generics.Ptr[RowType], RowType any] struct {
	container *Container[P, RowType]
	lock      sync.Mutex
	rootDir   string
	prefix    string
}

// NewMultiContainer creates a new MultiContainer
func NewMultiContainer[P generics.Ptr[RowType], RowType any](
	dir, prefix string,
	tc *task.TaskConfig,
) (c *MultiContainer[P, RowType], err error) {
	c = &MultiContainer[P, RowType]{
		rootDir: dir,
		prefix:  prefix,
	}

	if err = c.loadContainer(); err != nil {
		return nil, err
	}

	if tc != nil {
		tc.Do(c.controlArchive)
	}

	return
}

// Container
func (c *MultiContainer[P, RowType]) Container() *Container[P, RowType] {
	return c.container
}

// loadContainer
func (c *MultiContainer[P, RowType]) loadContainer() (err error) {
	var filename string
	if filename, err = c.getFilename(); err != nil {
		return err
	}

	if c.container, err = Load[P, RowType](filename, 1); err != nil {
		return err
	}

	return
}

// controlArchive
func (c *MultiContainer[P, RowType]) controlArchive(t *task.Task) error {
	var files []string

	c.lock.Lock()
	{
		c.container.Close()

		if err := c.loadContainer(); err != nil {
			return err
		}

		// glob *.sbt
		var err error
		if files, err = filepath.Glob(path.Join(c.rootDir, "*.sbt")); err != nil {
			return err
		}
	}
	c.lock.Unlock()

	// remove the last file which is the new one
	if len(files) > 1 {
		files = files[:len(files)-1]
	}

	for _, f := range files {
		if err := binary.EasyGzip(f); err != nil {
			return err
		}

		if err := os.Remove(f); err != nil {
			return err
		}
	}

	return nil
}

// Lock
func (c *MultiContainer[P, RowType]) Lock() {
	c.lock.Lock()
}

// Unlock
func (c *MultiContainer[P, RowType]) Unlock() {
	c.lock.Unlock()
}

// getFilename
func (c *MultiContainer[P, RowType]) getFilename() (filename string, err error) {
	var loc *time.Location
	if loc, err = time.LoadLocation("UTC"); err != nil {
		return "", err
	}

	now := time.Now().In(loc)

	filename = fmt.Sprintf("%s-%s_%d.sbt", c.prefix, now.Format("2006_01_02-15_04"), now.UnixNano())

	return path.Join(c.rootDir, filename), nil
}
