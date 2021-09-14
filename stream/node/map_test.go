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

func TestMap(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic()
	outTopic := essentials.NewTopic()
	s := streaming.NewStream(
		node.TopicSource(inTopic),
		node.Map(func(e event.Event) event.Event {
			return "Hello, " + e.(string) + "!"
		}),
		node.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- "Caravan"
	p.Close()

	c := outTopic.NewConsumer()
	greeting := receiver.MustReceive(c)
	c.Close()

	as.Equal("Hello, Caravan!", greeting)
	as.Nil(s.Stop())
}
