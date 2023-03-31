package concurrency

type Subscription[ChannelT comparable, MsgT any] struct {
	channel ChannelT
	// TODO: use buffered channel
	msgCh  chan MsgT
	broker *Broker[ChannelT, MsgT]
}

func NewSubscription[ChannelT comparable, MsgT any](broker *Broker[ChannelT, MsgT], channel ChannelT, msgCh chan MsgT) *Subscription[ChannelT, MsgT] {
	return &Subscription[ChannelT, MsgT]{
		channel: channel,
		msgCh:   msgCh,
		broker:  broker,
	}
}

// Channel returns the channel of the subscription.
func (s *Subscription[ChannelT, MsgT]) Channel() chan MsgT {
	return s.msgCh
}

// Close removes the subscription.
func (s *Subscription[ChannelT, MsgT]) Close() {
	s.broker.Unsubscribe(s)
}

// Broker returns the broker of the subscription.
func (s *Subscription[ChannelT, MsgT]) Broker() *Broker[ChannelT, MsgT] {
	return s.broker
}
