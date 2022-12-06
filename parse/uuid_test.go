//

package parse

import "testing"

func TestReadUUIDv4(t *testing.T) {
	value := "this is a test ccc398dc-f864-4d98-9a5d-71f54bfd9a85 uuid"
	expected := "ccc398dc-f864-4d98-9a5d-71f54bfd9a85"

	result, err := ExtractUUIDv4(value)
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}

	if result != expected {
		t.Fatalf("expected %s, got %s", expected, result)
	}
}
