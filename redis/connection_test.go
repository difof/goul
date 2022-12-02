package redis

import (
	"context"
	"testing"
)

func connect() (Connection, error) {
	return Connect(NewOptions().
		SetAddr("localhost:6379").
		SetPassword("jiopdqw21089ueDHOIUIUdo0wid210yh89onjkd109jc218JOIOHIUD981u2uibhkj").
		SetDB(0))
}

func TestConnect(t *testing.T) {
	ctx, err := connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	const testVal = "test214"

	cacheValue := testVal
	if err := ctx.SetCache("test", cacheValue, true); err != nil {
		t.Fatalf("set redis error: %v", err)
	}
	cacheValue = ""

	err = ctx.GetCache("test", &cacheValue)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if cacheValue != testVal {
		t.Fatalf("val is not test")
	}
}

func TestPubSub(t *testing.T) {
	ctx, err := connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	pubsub := ctx.Subscribe("test")
	defer pubsub.Close()

	const testVal = "test214"

	if err := ctx.Publish("test", testVal); err != nil {
		t.Fatalf("publish error: %v", err)
	}

	msg, err := pubsub.ReceiveMessage(context.Background())
	if err != nil {
		t.Fatalf("receive message error: %v", err)
	}

	if msg.Payload != testVal {
		t.Fatalf("val is not test")
	}
}

func TestSet(t *testing.T) {
	ctx, err := connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	ctx.Client.SAdd(context.Background(), "test", "1.1.1.1")
	ctx.Client.SAdd(context.Background(), "test", "2.2.2.2")
	ctx.Client.SAdd(context.Background(), "test", "4.4.4.4")

	result, err := ctx.Client.SIsMember(context.Background(), "test", "2.2.2.2").Result()
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if !result {
		t.Fatalf("val is not true")
	}
}

func TestPrefix(t *testing.T) {
	ctx, err := connect()
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}

	//if err := ctx.SetCache("base:a", "test", true); err != nil {
	//	t.Fatalf("set error: %v", err)
	//}
	//
	//if err := ctx.SetCache("base:b", "test", true); err != nil {
	//	t.Fatalf("set error: %v", err)
	//}
	//
	//if err := ctx.SetCache("base:c", "test", true); err != nil {
	//	t.Fatalf("set error: %v", err)
	//}

	if err := ctx.Client.Set(context.Background(), "base:a", "test", 0).Err(); err != nil {
		t.Fatalf("set error: %v", err)
	}

	if err := ctx.Client.Set(context.Background(), "base:b", "test", 0).Err(); err != nil {
		t.Fatalf("set error: %v", err)
	}

	if err := ctx.Client.Set(context.Background(), "base:c", "test", 0).Err(); err != nil {
		t.Fatalf("set error: %v", err)
	}

	keys, _, err := ctx.Client.Scan(context.Background(), 0, "base:*", 10).Result()
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
}
