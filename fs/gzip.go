package fs

import (
	"compress/gzip"
	"github.com/difof/goul/errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func GZipFile(inputFilename, outputFilename string) error {
	file, err := os.Open(inputFilename)
	if err != nil {
		return errors.Newif(err, "error opening file: %s", inputFilename)
	}
	defer file.Close()

	compressedFile, err := os.Create(outputFilename)
	if err != nil {
		return errors.Newif(err, "error creating compressed file: %s", outputFilename)
	}
	defer compressedFile.Close()

	gzipWriter := gzip.NewWriter(compressedFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, file)
	if err != nil {
		return errors.Newif(err, "error copying file to gzip writer")
	}

	return nil
}

func ExtractGZipFile(inputFilename, outputFilename string) (string, error) {
	file, err := os.Open(inputFilename)
	if err != nil {
		return "", errors.Newif(err, "error opening file: %s", inputFilename)
	}
	defer file.Close()

	decompressedFilePath := strings.TrimSuffix(inputFilename, filepath.Ext(inputFilename))
	decompressedFile, err := os.Create(decompressedFilePath)
	if err != nil {
		return "", errors.Newif(err, "error creating decompressed file: %s", inputFilename)
	}
	defer decompressedFile.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return "", errors.Newif(err, "error creating gzip reader: %s", inputFilename)
	}
	defer gzipReader.Close()

	_, err = io.Copy(decompressedFile, gzipReader)
	if err != nil {
		return "", errors.Newif(err, "error copying file to gzip writer: %s", inputFilename)
	}

	return decompressedFilePath, nil
}
