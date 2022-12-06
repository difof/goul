//

package parse

import (
	"fmt"
	"regexp"
)

var (
	UUIDv4RegexStr = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
	UUIDv4Regex    = regexp.MustCompile(UUIDv4RegexStr)
)

// ExtractUUIDv4 reads a UUIDv4 from a string.
func ExtractUUIDv4(s string) (string, error) {
	match := UUIDv4Regex.FindStringSubmatch(s)
	if match == nil || len(match) == 0 {
		return "", fmt.Errorf("no UUIDv4 found in %s", s)
	}

	return match[0], nil
}

// IsUUIDv4 checks if a string is a UUIDv4.
func IsUUIDv4(s string) bool {
	return UUIDv4Regex.MatchString(s)
}
