package node_test

import (
	"testing"

	"github.com/caravan/streaming/stream"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New()
	outTopic := topic.New()
	s := streaming.NewStream(
		node.TopicSource(inTopic),
		node.Map(func(e stream.Event) stream.Event {
			return "Hello, " + e.(string) + "!"
		}),
		node.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- "Caravan"
	p.Close()

	c := outTopic.NewConsumer()
	greeting := topic.MustReceive(c)
	c.Close()

	as.Equal("Hello, Caravan!", greeting)
	as.Nil(s.Stop())
}
