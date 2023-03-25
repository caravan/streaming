package node_test

import (
	"testing"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New()
	outTopic := topic.New()
	s := streaming.NewStream(
		node.TopicSource(inTopic),
		node.Filter(func(e stream.Event) bool {
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
	as.Equal(0, topic.MustReceive(c))
	as.Equal(2, topic.MustReceive(c))
	as.Equal(4, topic.MustReceive(c))
	as.Equal(6, topic.MustReceive(c))
	as.Equal(8, topic.MustReceive(c))
	c.Close()

	as.Nil(s.Stop())
}
