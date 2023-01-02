//

package concurrency

import "github.com/difof/goul/generics"

type StringChannelT string
type IntChannelT int
type EmptyMsgT *struct{}

// Broker is a message broadcaster to multiple subscribers (channels).
type Broker[ChannelT comparable, MsgT any] struct {
	stop           chan struct{}
	pub            chan generics.Tuple[ChannelT, MsgT]
	sub            chan *Subscription[ChannelT, MsgT]
	unsub          chan *Subscription[ChannelT, MsgT]
	defaultChannel ChannelT
}

// NewBroker creates a new Broker.
func NewBroker[ChannelT comparable, MsgT any](defaultChannel ChannelT) *Broker[ChannelT, MsgT] {
	return &Broker[ChannelT, MsgT]{
		stop:           make(chan struct{}),
		pub:            make(chan generics.Tuple[ChannelT, MsgT], 1),
		sub:            make(chan *Subscription[ChannelT, MsgT], 1),
		unsub:          make(chan *Subscription[ChannelT, MsgT], 1),
		defaultChannel: defaultChannel,
	}
}

// Start starts the broker. Must be called before adding any new subscribers.
func (b *Broker[ChannelT, MsgT]) Start() {
	subs := map[ChannelT]map[*Subscription[ChannelT, MsgT]]struct{}{}

	for {
		select {
		case <-b.stop:
			// TODO: broadcast stop message to all subscribers? or close all subs?
			return
		case sub := <-b.sub:
			if _, ok := subs[sub.channel]; !ok {
				subs[sub.channel] = map[*Subscription[ChannelT, MsgT]]struct{}{}
			}
			subs[sub.channel][sub] = struct{}{}
		case unsub := <-b.unsub:
			delete(subs[unsub.channel], unsub)
		case msg := <-b.pub:
			for sub := range subs[msg.First] {
				select {
				case sub.msgCh <- msg.Second:
				default:
				}
			}
		}
	}
}

// Stop stops the broker.
func (b *Broker[ChannelT, MsgT]) Stop() {
	close(b.stop)
}

// Publish publishes a message to the broker on default channel.
func (b *Broker[ChannelT, MsgT]) Publish(msg MsgT) {
	b.pub <- generics.NewTuple(b.defaultChannel, msg)
}

// PublishChannel publishes a message to the broker.
func (b *Broker[ChannelT, MsgT]) PublishChannel(channel ChannelT, msg MsgT) {
	b.pub <- generics.NewTuple(channel, msg)
}

// Subscribe subscribes to the broker on default channel.
func (b *Broker[ChannelT, MsgT]) Subscribe() *Subscription[ChannelT, MsgT] {
	return b.SubscribeChannel(b.defaultChannel)
}

// SubscribeChannel subscribes to the broker.
func (b *Broker[ChannelT, MsgT]) SubscribeChannel(channel ChannelT) *Subscription[ChannelT, MsgT] {
	sub := NewSubscription(b, channel, make(chan MsgT, 5))
	b.sub <- sub
	return sub
}

// Unsubscribe unsubscribes from the broker.
func (b *Broker[ChannelT, MsgT]) Unsubscribe(sub *Subscription[ChannelT, MsgT]) {
	b.unsub <- sub
	close(sub.msgCh)
}
