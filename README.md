# Goul

Go utility library for my personal projects.

Docs: [https://pkg.go.dev/github.com/difof/goul](https://pkg.go.dev/github.com/difof/goul)

## Features

I will add features as I need them.

| Feature                                          | Description                                                      |
|--------------------------------------------------|------------------------------------------------------------------|
| [Config loader](./config_loader/loader_test.go)  | Load and combine configuration from json/yaml and env            |
| [Broker](./concurrency/broker_test.go)           | Local broker pattern                                             |
| [Generic collections](./generics)                | Generic collections with LINQ capabilities                       |
| [LINQ for slices](./generics/native_linq.go)     | Basic LINQ support for native slices                             |
| [Local task scheduler](./task/scheduler_test.go) | Schedule tasks to run at a specific time, once or repeatedly     |
| [Redis](./redis)                                 | Redis connection helper                                          |
| [Bots](./bot)                                    | Bot utilities ([Telegram](./bot/tgbot/bot_test.go) only for now) |

## Usage

`go get github.com/difof/goul`

## TODO

- All collections from GoDS
- JSON serializable collections
- Stringer collections
