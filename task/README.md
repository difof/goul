# Task module

## Task runner

Files: [task_config.go](./task_config.go), [task_runner.go](./task_runner.go), [task_test.go](./task_test.go), [task.go](./task.go)

Task scheduler is a simple scheduler that runs tasks in a given interval. It is used to run tasks that need to be run periodically.

## Job queue

Files: [job_queue.go](./job_queue.go)

Job queue is a simple queue that can be used to queue jobs that need to be run in a separate goroutine.

## Retry logic

File: [retry.go](./retry.go)

This file has a single `Retry` function which retries the given config if it fails.

## Command runner

TODO
