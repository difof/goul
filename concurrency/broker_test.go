//

package concurrency

import (
	"fmt"
	"github.com/difof/goul/generics"
	"testing"
	"time"
)

func TestNewBroker(t *testing.T) {
	b := NewBroker[string, string]("::")
	go b.Start()

	numSubs := 20
	subReceived := generics.NewSafeMap[int, bool]()

	// create and subscribe 3 clients:
	subFactory := func(id int) {
		sub := b.SubscribeChannel("testchannel")
		for range sub.Channel() {
			subReceived.Set(id, true)
		}
	}

	for i := 0; i < numSubs; i++ {
		go subFactory(i)
	}

	// wait for subscribers to fully subscribe!
	time.Sleep(10 * time.Millisecond)

	// start publishing messages:
	go func() {
		for msgId := 0; ; msgId++ {
			b.PublishChannel("testchannel", fmt.Sprintf("msg#%d", msgId))
			time.Sleep(time.Millisecond)
		}
	}()

	// let the messages flow for a while:
	time.Sleep(time.Second)
	b.Stop()

	unhandledClients := make([]int, 0, numSubs)
	handledClients := make([]int, 0, numSubs)
	for i := 0; i < numSubs; i++ {
		if _, ok := subReceived.Get(i); !ok {
			unhandledClients = append(unhandledClients, i)
		} else {
			handledClients = append(handledClients, i)
		}
	}

	if subReceived.Len() != numSubs {
		t.Logf("Expected %d clients to receive messages, but got %d", numSubs, subReceived.Len())
		t.Logf("Unhandled clients: %v", unhandledClients)
		t.Logf("Handled clients: %v", handledClients)
		t.Fail()
	}
}

func TestBroker_Unsubscribe(t *testing.T) {
	b := NewBroker[string, struct{}]("::")
	go b.Start()

	// id -> num received
	receives := generics.NewSafeMap[int, int]()

	subFactory := func(id int, ch chan struct{}) {
		for range ch {
			receives.Set(id, receives.MustGet(id)+1)
		}
	}

	numSubs := 20
	subs := make([]*Subscription[string, struct{}], 0, numSubs)
	for i := 0; i < numSubs; i++ {
		sub := b.SubscribeChannel("testchannel")
		subs = append(subs, sub)
		receives.Set(i, 0)
		go subFactory(i, sub.Channel())
	}

	b.PublishChannel("testchannel", struct{}{})
	time.Sleep(time.Second)
	for _, sub := range subs {
		sub.Close()
	}
	time.Sleep(time.Second)
	b.PublishChannel("testchannel", struct{}{})

	for i := 0; i < numSubs; i++ {
		if receives.MustGet(i) != 1 {
			t.Logf("Expected client %d to receive 1 message, but got %d", i, receives.MustGet(i))
			t.Fail()
		}
	}
}
