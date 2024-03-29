package errors

import (
	goerrors "errors"
	"testing"
)

func a() error {
	return Newif(b(), "in 'a' error from 'b'")
}

func b() error {
	return Newif(c(), "in 'b' error from 'c'")
}

func c() error {
	return Newif(messageError(), "in 'c' error from 'messageError'")
}

var msgErr = goerrors.New("message error")
var testErr = New("message error")

func messageError() error {
	return msgErr
}

func TestHasError(t *testing.T) {
	err := func() error {
		return Newi(a())
	}()

	err = Newi(err)

	if !Is(err, msgErr) {
		t.Fatal("err does not contain msgErr")
	}

	if Is(err, testErr) {
		t.Fatal("err contains testErr")
	}

	// order of stacktrace:
	// 1. messageError (final error)
	// 2. c
	// 3. b
	// 4. a
	// 5. TestHasError.func1
	// 6. TestHasError
	t.Log(err.Error())
}
