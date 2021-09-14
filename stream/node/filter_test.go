package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/receiver"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic()
	outTopic := essentials.NewTopic()
	s := streaming.NewStream(
		node.TopicSource(inTopic),
		node.Filter(func(e event.Event) bool {
			return e.(int)%2 == 0
		}),
		node.TopicSink(outTopic),
	)
	as.Nil(s.Start())

	p := inTopic.NewProducer()
	for i := 0; i < 10; i++ {
		p.Send() <- i
	}
	p.Close()

	c := outTopic.NewConsumer()
	as.Equal(0, receiver.MustReceive(c))
	as.Equal(2, receiver.MustReceive(c))
	as.Equal(4, receiver.MustReceive(c))
	as.Equal(6, receiver.MustReceive(c))
	as.Equal(8, receiver.MustReceive(c))
	c.Close()

	as.Nil(s.Stop())
}
