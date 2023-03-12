package concurrency

import "context"

type CancelContext struct {
	context.Context
	cancel context.CancelFunc
}

func NewCancelContext(parent context.Context) CancelContext {
	ctx, cancel := context.WithCancel(parent)
	return CancelContext{
		Context: ctx,
		cancel:  cancel,
	}
}

func (c CancelContext) Cancel() {
	c.cancel()
}
