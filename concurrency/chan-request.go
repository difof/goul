package concurrency

type ChanRequest[T any] struct {
	resp         chan T
	req          chan struct{}
	errorHandler func(err error)
}

func NewChanRequester[T any]() *ChanRequest[T] {
	return &ChanRequest[T]{resp: make(chan T, 10), req: make(chan struct{}, 10)}
}

// SetErrorHandler sets the error handler.
func (c *ChanRequest[T]) SetErrorHandler(f func(err error)) {
	c.errorHandler = f
}

// Request is a blocking function.
func (c *ChanRequest[T]) Request() (resp T) {
	c.req <- struct{}{}
	return <-c.resp
}

// Handle is a blocking function.
func (c *ChanRequest[T]) Handle(f func() (T, error)) {
	for {
		<-c.req
		r, err := f()
		if err != nil {
			c.errorHandler(err)
			return
		}
		c.resp <- r
	}
}
