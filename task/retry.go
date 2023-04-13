package task

import (
	"context"
	"time"
)

type RetryBackoff func(retry int) time.Duration
type RetryCallback func(count int) (stop bool, err error)

func Retry(
	ctx context.Context,
	maxRetry int,
	callback RetryCallback,
	backoff RetryBackoff,
	errHook func(err error),
) (err error) {
	tk := time.NewTicker(1 * time.Second)
	defer tk.Stop()

	var stop bool
	for i := 0; i < maxRetry; i++ {
		stop, err = callback(i)
		if err != nil && errHook != nil {
			errHook(err)
		}

		if stop || err == nil {
			return
		}

		tk.Reset(backoff(i))
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
	}

	return
}
