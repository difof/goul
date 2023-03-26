package concurrency

import "sync"

// ErrFunc is called when an error occurs in a goroutine.
// If it returns true, the context is cancelled.
type ErrFunc func(error) bool

// ErrorHandler is used to handle errors in goroutines
type ErrorHandler struct {
	ctx  CancelContext
	wg   sync.WaitGroup
	last error
	ef   ErrFunc
	errs chan error
}

func NewErrorHandler(ctx CancelContext) *ErrorHandler {
	return &ErrorHandler{
		ctx:  ctx,
		errs: make(chan error, 10),
	}
}

// SetErrorHandler sets the error handler function
func (h *ErrorHandler) SetErrorHandler(ef ErrFunc) {
	h.ef = ef
}

// Done returns a channel that is closed when the context is cancelled
func (h *ErrorHandler) Done() <-chan struct{} {
	return h.ctx.Done()
}

// Context returns the context
func (h *ErrorHandler) Context() CancelContext {
	return h.ctx
}

// Go runs a function in a goroutine
func (h *ErrorHandler) Go(f func() error) {
	h.wg.Add(1)
	go func() {
		var err error
		defer func() {
			if err != nil {
				h.errs <- err
			}
		}()
		defer h.wg.Done()

		if err = f(); err != nil {
			h.last = err

			if h.ef != nil {
				if h.ef(err) {
					h.ctx.Cancel()
				}
			} else {
				h.ctx.Cancel()
			}
		}
	}()
}

// Fail cancels the context with given error
func (h *ErrorHandler) Fail(err error) {
	h.last = err
	h.ctx.Cancel()
}

// Wait signal close and waits for all goroutines to finish
func (h *ErrorHandler) Wait() error {
	h.ctx.Cancel()
	h.wg.Wait()
	return h.last
}

// Error returns the error channel
func (h *ErrorHandler) Error() <-chan error {
	return h.errs
}
