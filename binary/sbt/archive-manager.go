package sbt

import (
	"fmt"
	"github.com/difof/goul/binary"
	"os"
)

// ArchiveManager manages the archive of a container
type ArchiveManager struct {
	archiveFilename      string
	decompressedFilename string
}

func LoadArchive(archiveFilename string) (am *ArchiveManager, err error) {
	am = &ArchiveManager{
		archiveFilename: archiveFilename,
	}

	am.decompressedFilename, err = binary.EasyUnGzip(archiveFilename)
	if err != nil {
		err = fmt.Errorf("LoadArchive error %s: %w", archiveFilename, err)
		return
	}

	return
}

// Close closes the archive
func (am *ArchiveManager) Close() error {
	if err := os.Remove(am.decompressedFilename); err != nil {
		return err
	}

	return nil
}

// ArchiveFilename get the archive filename
func (am *ArchiveManager) ArchiveFilename() string {
	return am.archiveFilename
}

// DecompressedFilename get the decompressed filename
func (am *ArchiveManager) DecompressedFilename() string {
	return am.decompressedFilename
}
