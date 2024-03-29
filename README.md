# Goul

Generic Golang utility library.

Docs: [pkg.go.dev](https://pkg.go.dev/github.com/difof/goul)

## Features

I will add features as I need them.

| Feature                                          | Description                                                           |
|--------------------------------------------------|-----------------------------------------------------------------------|
| [Config loader](./config_loader/loader_test.go)  | Load and combine configuration from json/yaml and env                 |
| [Broker](./concurrency/broker_test.go)           | Local broker pattern                                                  |
| [Fast and efficient CSV alternative](binary/sbt) | Fast binary storage for bulk read/write of fixed size structured data |
| [Local task scheduler](./task/task_test.go)      | Schedule tasks to run at a specific time, once or repeatedly          |
| [Job queue](./task/job_queue.go)                 | Simple job queue                                                      |
| [Generic collections](./generics)                | Generic collections with LINQ capabilities                            |
| [LINQ for slices](./generics/native_linq.go)     | Basic LINQ support for native slices                                  |
| [Redis](ext/redis)                               | Redis connection helper                                               |
| [Bots](ext/bot)                                  | Bot utilities ([Telegram](ext/bot/tgbot/bot_test.go) only, for now)   |
| [Errors](./errors)                               | Improved error handling                                               |

## Usage

`go get github.com/difof/goul`

## TODO

- All collections from GoDS
- JSON serializable collections
- Stringer collections
- Cmdrunner package
