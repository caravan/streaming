package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
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
		node.Filter(func(e message.Event) bool {
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
	as.Equal(0, message.MustReceive(c))
	as.Equal(2, message.MustReceive(c))
	as.Equal(4, message.MustReceive(c))
	as.Equal(6, message.MustReceive(c))
	as.Equal(8, message.MustReceive(c))
	c.Close()

	as.Nil(s.Stop())
}
