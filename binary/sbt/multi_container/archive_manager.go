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
func NewArchiveManager(rootDir, prefix string, compQueueBufSize, compPoolSize int) (am *ArchiveManager, err error) {
	am = &ArchiveManager{
		rootDir:          rootDir,
		prefix:           prefix,
		compressionQueue: make(chan string, compQueueBufSize),
		errs:             new(errgroup.Group),
	}

	am.stopContext, am.stopFunc = context.WithCancel(context.Background())

	for i := 0; i < compPoolSize; i++ {
		am.errs.Go(am.manageCompressionQueue)
	}

	err = am.CleanCompress()

	return
}

// CleanCompress does this to root dir:
//
//   - Ignore files with prefix and .sbt extension if there's a compressed version
//   - Compress any file with prefix and .sbt extension
func (am *ArchiveManager) CleanCompress() error {
	compressed, err := am.globPrefixed(".sbt.gz")
	if err != nil {
		return err
	}

	files, err := am.globPrefixed(".sbt")
	if err != nil {
		return err
	}

	toCompress := make([]string, 0, len(files))

	for fi, file := range files {
		hasCompressed := false
		for _, compressedFile := range compressed {
			if strings.TrimSuffix(file, ".sbt") == strings.TrimSuffix(compressedFile, ".sbt.gz") {
				hasCompressed = true
				break
			}
		}

		// ignore the last file if it's not compressed
		if !hasCompressed {
			if fi == len(files)-1 {
				break
			}

			toCompress = append(toCompress, file)
		}
	}

	for _, file := range toCompress {
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
				return err
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

// getLastUncompressedFilename returns the last uncompressed filename
func (am *ArchiveManager) getLastUncompressedFilename() (filename string, err error) {
	files, err := am.globPrefixed(".sbt")
	if err != nil {
		err = fmt.Errorf("getLastUncompressedFilename glob error: %w", err)
		return
	}

	if len(files) == 0 {
		return
	}

	filename = files[len(files)-1]

	// check .sbt.gz with same name exists
	compressedFilename := strings.TrimSuffix(filename, ".sbt") + ".sbt.gz"
	if _, err = os.Stat(compressedFilename); os.IsNotExist(err) {
		err = nil
		return
	} else if err != nil {
		err = fmt.Errorf("getLastUncompressedFilename stat error: %w", err)
		return
	}

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

// Files returns the decompressed filename channel.
// This function panics on error, make sure to recover.
func (am *ArchiveManager) Files(ctx context.Context, start, end time.Time) chan string {
	iterableFilenames, err := am.getIterableFilenames(start, end)
	if err != nil {
		return nil
	}

	if len(iterableFilenames) == 0 {
		return nil
	}

	availableChan := make(chan string, 1)

	// (decompress files and) send iterable filenames to availableChan
	am.errs.Go(func() (err error) {
		defer close(availableChan)

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

	return availableChan
}
