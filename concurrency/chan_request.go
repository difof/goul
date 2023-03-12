package concurrency

import "context"

// ChanRequest is a simple implementation of request-response pattern used to request data from goroutines.
// Managing the Handler goroutine is the responsibility of the user.
type ChanRequest[Req, Res any] struct {
	resp chan Res
	req  chan Req
}

func NewChanRequest[Req, Res any]() *ChanRequest[Req, Res] {
	return &ChanRequest[Req, Res]{resp: make(chan Res, 10), req: make(chan Req, 10)}
}

// Request is a blocking function.
func (c *ChanRequest[Req, Res]) Request(arg Req) (resp Res) {
	c.req <- arg
	return <-c.resp
}

// Handle is a blocking function.
func (c *ChanRequest[Req, Res]) Handle(ctx context.Context, f func(arg Req) (Res, error)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case arg := <-c.req:
			r, err := f(arg)
			if err != nil {
				return err
			}
			c.resp <- r
		}
	}
}
