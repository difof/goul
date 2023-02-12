package binary

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

func EasyGzip(filePath string) error {
	// Open the original file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a new file with .gz extension
	compressedFilePath := filePath + ".gz"
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

func EasyUnGzip(filePath string) (string, error) {
	// Open the original file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a new file with .gz extension
	decompressedFilePath := filePath + ".decompressed"
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
