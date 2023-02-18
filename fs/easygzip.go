package fs

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func EasyGzip(filename string) error {
	// Open the original file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a new file with .gz extension
	compressedFilePath := filename + ".gz"
	compressedFile, err := os.Create(compressedFilePath)
	if err != nil {
		return fmt.Errorf("error creating compressed file: %w", err)
	}
	defer compressedFile.Close()

	// Use gzip writer to compress the file
	gzipWriter := gzip.NewWriter(compressedFile)
	defer gzipWriter.Close()

	// Copy the content of the original file to the gzip writer
	_, err = io.Copy(gzipWriter, file)
	if err != nil {
		return fmt.Errorf("error copying file to gzip writer: %w", err)
	}

	return nil
}

func EasyUnGzip(filename string) (string, error) {
	// Open the original file
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a new file with .gz extension
	decompressedFilePath := strings.TrimSuffix(filename, filepath.Ext(filename)) + ".decompressed"
	decompressedFile, err := os.Create(decompressedFilePath)
	if err != nil {
		return "", fmt.Errorf("error creating decompressed file: %w", err)
	}
	defer decompressedFile.Close()

	// Use gzip writer to compress the file
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return "", fmt.Errorf("error creating gzip reader: %w", err)
	}
	defer gzipReader.Close()

	// Copy the content of the original file to the gzip writer
	_, err = io.Copy(decompressedFile, gzipReader)
	if err != nil {
		return "", fmt.Errorf("error copying file to gzip writer: %w", err)
	}

	return decompressedFilePath, nil
}
