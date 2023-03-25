package topic

import (
	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
)

type (
	Topic    topic.Topic[stream.Event]
	Producer topic.Producer[stream.Event]
	Consumer topic.Consumer[stream.Event]
)

var (
	msg         = message.Of[stream.Event]()
	Poll        = msg.Poll
	Receive     = msg.Receive
	MustReceive = msg.MustReceive
	Send        = msg.Send
	MustSend    = msg.MustSend
)

func New() Topic {
	return essentials.NewTopic[stream.Event]()
}
