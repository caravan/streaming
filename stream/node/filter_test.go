package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	as := assert.New(t)

	typed := streaming.Of[int]()
	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()
	s := typed.NewStream(
		typed.TopicSource(inTopic),
		typed.Filter(func(m int) bool {
			return m%2 == 0
		}),
		typed.TopicSink(outTopic),
	)
	as.Nil(s.Start())

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
