package task

import (
	"context"
	"time"
)

func Retry(
	ctx context.Context,
	count, delaySec int,
	backofffn func(count int) (delaySec int),
	fn func(count int) (stop bool, err error),
	errfn func(err error)) (err error) {
	tk := time.NewTicker(time.Duration(delaySec) * time.Second)
	defer tk.Stop()

	var stop bool
	for i := 0; i < count; i++ {
		stop, err = fn(i)
		if err != nil && errfn != nil {
			errfn(err)
			err = nil
		}

		if stop || err == nil {
			return
		}

		tk.Reset(time.Duration(backofffn(i)) * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
	}

	return
}
