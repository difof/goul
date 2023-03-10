package errors

import (
	"io"
	"testing"
)

func a() error {
	return Newi(b(), "failed to call b")
}

func b() error {
	return Newi(c(), "failed to call c")
}

func c() error {
	return Newi(io.EOF, "failed to call io.EOF")
}

func TestHasError(t *testing.T) {
	err := a()

	if !Is(err, io.EOF) {
		t.Error("err doesn't contain io.EOF")
	}

	t.Log(err.Error())
}
