package node_test

import (
	"testing"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic()
	outTopic := caravan.NewTopic()
	s := caravan.NewStream(
		node.TopicSource(inTopic),
		node.Filter(func(e topic.Event) bool {
			return e.(int)%2 == 0
		}),
		node.TopicSink(outTopic),
	)
	as.Nil(s.Start())

	p := inTopic.NewProducer()
	for i := 0; i < 10; i++ {
		p.Send(i)
	}
	_ = p.Close()

	c := outTopic.NewConsumer()
	as.Equal(0, topic.MustReceive(c))
	as.Equal(2, topic.MustReceive(c))
	as.Equal(4, topic.MustReceive(c))
	as.Equal(6, topic.MustReceive(c))
	as.Equal(8, topic.MustReceive(c))
	_ = c.Close()

	as.Nil(s.Stop())
}
