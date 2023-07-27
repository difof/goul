# Concurrency module

## Broker

Files: [broker.go](./broker.go), [subscription.go](./subscription.go), [broker_test.go](./broker_test.go)

Simple broker implementation. It allows to send messages to all subscribers.

## Chan request

File: [chan_request.go](./chan_request.go)

Simple request-response pattern implementation using channels.

## Error handler

File: [error_handler.go](./error_handler.go)

Simple error handler for goroutines. Similar to `sync.WaitGroup` and `golang.org/x/sync/errgroup` but for handling errors and closing concurrent types.

## Semaphore

File: [semaphore.go](./semaphore.go)

Simple semaphore implementation.

## Cancel context

File: [cancel_context.go](./cancel_context.go)

Easy wrapper for cancellable context.
