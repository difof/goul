# Errors

Improved error handling compatible with standard library

```go
package main

import (
	"fmt"
    goerr "errors"
	"github.com/difof/goul/errors"
)

func someerrfunc() error {
	return goerr.New("some error")
}

func DoSomething() error {
	return errors.Newi(someerrfunc(), "something went wrong") 
}

func DoSomethingTwoReturns() error {
	return errors.CheckAny(TwoReturns())(
		// This callback is called if TwoReturns() returns result, nil
		func(result int) error {
			// do something with result
			return nil
		})
}

func TwoReturns() (int, error) {
	result := 223
	return result, errors.Newm("something went wrong")
}

func main() {
	// Simple use which prints stack trace
	if err := DoSomething(); err != nil {
		fmt.Println(err)
	}

	// Dual return values
	if err := DoSomethingTwoReturns(); err != nil {
		fmt.Println(err)
	}
}
```