package multi_container

import (
	"context"
	"fmt"
	"github.com/difof/goul/fs"
	"golang.org/x/sync/errgroup"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ArchiveManager manages the archives of the multi container
type ArchiveManager struct {
	rootDir          string
	prefix           string
	compressionQueue chan string
	errs             *errgroup.Group
	stopContext      context.Context
	stopFunc         context.CancelFunc
}

// NewArchiveManager creates a new archive manager
func NewArchiveManager(rootDir, prefix string, buffersz int) (am *ArchiveManager, err error) {
	am = &ArchiveManager{
		rootDir:          rootDir,
		prefix:           prefix,
		compressionQueue: make(chan string, buffersz),
		errs:             new(errgroup.Group),
	}

	am.stopContext, am.stopFunc = context.WithCancel(context.Background())
	am.errs.Go(am.manageCompressionQueue)

	err = am.CompressRemaining()

	return
}

// CompressRemaining compresses all remaining files
func (am *ArchiveManager) CompressRemaining() error {
	files, err := am.globPrefixed(".sbt")
	if err != nil {
		return err
	}

	for _, file := range files {
		am.QueueCompression(file)
	}

	return nil
}

// QueueCompression queues files for compression. filename should be the full path to the file (starting at rootDir)
func (am *ArchiveManager) QueueCompression(filename string) {
	am.compressionQueue <- filename
}

// manageCompressionQueue manages the compression queue
func (am *ArchiveManager) manageCompressionQueue() error {
	for {
		select {
		case <-am.stopContext.Done():
			return nil
		case filename := <-am.compressionQueue:
			if err := am.compressFile(filename); err != nil {
				return fmt.Errorf("failed to compress file %s: %w", filename, err)
			}
		}
	}
}

// compressFile compresses a file
func (am *ArchiveManager) compressFile(filename string) error {
	if err := fs.EasyGzip(filename); err != nil {
		return fmt.Errorf("failed to compress file %s: %w", filename, err)
	}

	if err := os.Remove(filename); err != nil {
		return fmt.Errorf("failed to remove file %s: %w", filename, err)
	}

	return nil
}

// decompressFile decompresses a file
func (am *ArchiveManager) decompressFile(filename string) (out string, err error) {
	out, err = fs.EasyUnGzip(filename)
	if err != nil {
		err = fmt.Errorf("failed to decompress file %s: %w", filename, err)
		return
	}

	return
}

// globPrefixed asc sorted with prefix and suffix (extension)
func (am *ArchiveManager) globPrefixed(suffix string) (files []string, err error) {
	pattern := am.prefix + "*" + suffix
	if files, err = filepath.Glob(path.Join(am.rootDir, pattern)); err != nil {
		err = fmt.Errorf("globPrefixed %s error: %w", pattern, err)
		return
	}

	sort.Strings(files)

	return
}

// getIterableFilenames returns all filenames that are iterable. Used by IterHandler.
func (am *ArchiveManager) getIterableFilenames(start, end time.Time) (files []string, err error) {
	allFiles, err := am.globPrefixed(".sbt*")
	if err != nil {
		err = fmt.Errorf("getIterableFilenames glob all error: %w", err)
		return
	}

	// group by filename
	mapped := map[string][]string{}
	for _, f := range allFiles {
		absFilename := f
		switch filepath.Ext(f) {
		case ".sbt":
			absFilename = strings.TrimSuffix(f, ".sbt")
		case ".gz":
			absFilename = strings.TrimSuffix(f, ".sbt.gz")
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

		for _, ext := range []string{".sbt", ".sbt.gz"} {
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

		if parts.Prefix() != am.prefix || parts.Date().After(end) || parts.Date().Before(start) {
			continue
		}

		files = append(files, f)
	}

	return
}

// Close closes the archive
func (am *ArchiveManager) Close() error {
	am.stopFunc()
	return am.errs.Wait()
}

// FullChan returns the decompressed filename channel.
// This function panics on error, make sure to recover.
func (am *ArchiveManager) FullChan(ctx context.Context, start, end time.Time) chan string {
	iterableFilenames, err := am.getIterableFilenames(start, end)
	if err != nil {
		return nil
	}

	if len(iterableFilenames) == 0 {
		return nil
	}

	availableChan := make(chan string, 2)
	filenameChan := make(chan string, 1)

	am.errs.Go(func() (err error) {
		for _, filename := range iterableFilenames {
			if filepath.Ext(filename) == ".gz" {
				filename, err = am.decompressFile(filename)
				if err != nil {
					return err
				}
			}

			select {
			case <-ctx.Done():
				return
			case availableChan <- filename:
			}
		}

		return
	})

	go func() {
		defer close(filenameChan)

		for {
			select {
			case <-ctx.Done():
				return
			case fn := <-availableChan:
				select {
				case <-ctx.Done():
					return
				case filenameChan <- fn:
				}
			}
		}
	}()

	return filenameChan
}
