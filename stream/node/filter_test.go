package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/stream"
)

func TestFilter(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()

	s := internal.Make(
		node.TopicConsumer(inTopic),
		node.Subprocess(
			node.Filter(func(m int) bool {
				return m%2 == 0
			}),
			node.TopicProducer(outTopic),
		),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	for i := 0; i < 10; i++ {
		p.Send() <- i
	}
	p.Close()

	c := outTopic.NewConsumer()
	as.Equal(0, <-c.Receive())
	as.Equal(2, <-c.Receive())
	as.Equal(4, <-c.Receive())
	as.Equal(6, <-c.Receive())
	as.Equal(8, <-c.Receive())
	c.Close()

	as.Nil(s.Stop())
}
