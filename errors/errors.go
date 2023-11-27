package errors

import (
	"errors"
	goerrors "errors"
	"fmt"
	"runtime"
	"strings"
)

// Error is a lightweight error struct with stack trace.
// Compatible with standard errors package.
//
// Use NewXY functions to construct new errors.
type Error struct {
	Source  string
	Message error
	Inner   error
}

func NewError(source string, message, inner error) *Error {
	return &Error{
		Source:  source,
		Message: message,
		Inner:   inner,
	}
}

// Each iterates all inner errors as long as they're Error, starting from itself
func (e *Error) Each(it func(err error) bool) {
	if it == nil {
		return
	}

	var current error = e
	for current != nil {
		if !it(current) {
			break
		}

		var cast *Error
		if As(current, &cast) {
			current = cast.Unwrap()
		} else {
			current = nil
		}
	}
}

// StackTrace builds the stack trace of all inner errors of Error
func (e *Error) StackTrace() (list []string) {
	// TODO: should put the inner most error at first line then add stacktrace from outer most? maybe?
	list = make([]string, 0, 5)

	// reverse list
	defer func() {
		for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
			list[i], list[j] = list[j], list[i]
		}
	}()

	e.Each(func(err error) bool {
		if e, ok := err.(*Error); ok {
			list = append(list, e.String())
		} else {
			list = append(list, err.Error())
		}
		return true
	})

	return
}

// String returns current error's message and source
func (e *Error) String() string {
	if e.Message == nil {
		return e.Source
	}
	return fmt.Sprintf("%v: %v", e.Source, e.Message)
}

// Error returns the stack trace of this error
func (e *Error) Error() string {
	return strings.Join(e.StackTrace(), "\n")
}

// Unwrap returns the inner error
func (e *Error) Unwrap() error { return e.Inner }

// getCallerPath returns the file and line that called any of New functions as string.
//
// skipFrames parameter defines how many functions to skip.
func getCallerPath(skipFrames int) string {
	_, file, line, ok := runtime.Caller(2 + skipFrames)
	if !ok {
		return "<no source>"
	}

	return fmt.Sprintf("%v:%v", file, line)
}

// Catch returns a new error if the given error is not nil, otherwise returns nil.
//
// Useful for returning error or nil as last statement.
func Catch(err error) error {
	if err != nil {
		return News(1, err)
	}
	return nil
}

// Catchf is same as Catch except that it accepts a message
func Catchf(err error, msg string, params ...interface{}) error {
	if err != nil {
		msg = fmt.Sprintf(msg, params...)
		return Newsi(1, err, msg)
	}

	return nil
}

// IgnoreCatchResult is used in CatchResult callback to ignore the result
func IgnoreCatchResult[R any]() func(R) error { return func(R) error { return nil } }

// CatchResult is used for two return values functions returning an error.
//
// You should call the returned function,
// callback will be called if error is nil, otherwise it returns the error.
// Also returns the error returned by the given function.
//
// This function is a shortcut for when you either return an error or handle a result as the last statement.
func CatchResult[R any](result R, err error) func(func(R) error) error {
	if err != nil {
		return func(f func(result R) error) error {
			return err
		}
	}

	return func(f func(result R) error) (err error) {
		if err = f(result); err != nil {
			return News(1, f(result))
		}

		return
	}
}

// CatchResultf is same as CatchResult except that it appends a format message to the error.
func CatchResultf[R any](result R, err error) func(func(R) error, string, ...any) error {
	if err != nil {
		return func(f func(result R) error, format string, params ...any) error {
			return Newsif(1, err, format, params...)
		}
	}

	return func(f func(result R) error, format string, params ...any) (err error) {
		if err = f(result); err != nil {
			return Newsif(1, f(result), format, params...)
		}

		return
	}
}

// New adds stack trace to the given error
func New(err error) error {
	return NewError(getCallerPath(0), nil, err)
}

// Newm constructs a new Error using the message
func Newm(msg string) error {
	return NewError(getCallerPath(0), errors.New(msg), nil)
}

// Newi wraps the error with a new Error and a message
func Newi(inner error, msg string) error {
	return NewError(getCallerPath(0), errors.New(msg), inner)
}

// Newf constructs a new Error using the format
func Newf(format string, params ...interface{}) error {
	return NewError(getCallerPath(0), errors.New(fmt.Sprintf(format, params...)), nil)
}

// Newif constructs a new formatted error with a wrapped inner error
func Newif(inner error, format string, params ...interface{}) error {
	return NewError(getCallerPath(0), errors.New(fmt.Sprintf(format, params...)), inner)
}

// News constructs a new Error and skips stack frames for getting caller path.
func News(skip int, err error) error {
	return NewError(getCallerPath(skip), nil, err)
}

func Newsi(skip int, inner error, msg string) error {
	return NewError(getCallerPath(skip), errors.New(msg), inner)
}

func Newsf(skip int, format string, params ...interface{}) error {
	return NewError(getCallerPath(skip), errors.New(fmt.Sprintf(format, params...)), nil)
}

func Newsif(skip int, inner error, format string, params ...interface{}) error {
	return NewError(getCallerPath(skip), errors.New(fmt.Sprintf(format, params...)), inner)
}

// As is a wrapper around go's standard errors.As
func As(err error, target interface{}) bool { return goerrors.As(err, target) }

// Is is a wrapper around go's standard errors.Is
func Is(err, target error) bool { return goerrors.Is(err, target) }

// Unwrap is a wrapper around go's standard errors.Unwrap
func Unwrap(err error) error { return goerrors.Unwrap(err) }
