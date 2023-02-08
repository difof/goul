package fs

import "os"

// Exists returns whether the given file or directory exists or not
func Exists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	return true
}

// MakeDirIfNotExists creates a directory if it does not exist.
func MakeDirIfNotExists(path string) error {
	if !Exists(path) {
		return os.Mkdir(path, os.ModePerm)
	}

	return nil
}
