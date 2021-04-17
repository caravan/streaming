package node_test

import (
	"testing"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic()
	outTopic := caravan.NewTopic()
	s := streaming.NewStream(
		node.TopicSource(inTopic),
		node.Map(func(e topic.Event) topic.Event {
			return "Hello, " + e.(string) + "!"
		}),
		node.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send("Caravan")
	_ = p.Close()

	c := outTopic.NewConsumer()
	greeting := topic.MustReceive(c)
	_ = c.Close()

	as.Equal("Hello, Caravan!", greeting)
	as.Nil(s.Stop())
}
