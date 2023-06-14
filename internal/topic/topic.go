package topic

import (
	"github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
)

type (
	Topic[Msg any]    topic.Topic[Msg]
	Producer[Msg any] topic.Producer[Msg]
	Consumer[Msg any] topic.Consumer[Msg]
)

func New[Msg any]() Topic[Msg] {
	return essentials.NewTopic[Msg]()
}
