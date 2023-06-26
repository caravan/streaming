package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()
	typed := streaming.Of[int]()
	s := typed.NewStream(
		typed.TopicConsumer(inTopic),
		typed.Merge(
			node.Map(func(i int) int {
				return i + 1
			}),
			node.Map(func(i int) int {
				return i * 2
			}),
		),
		typed.TopicProducer(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	c := outTopic.NewConsumer()
	in := <-c.Receive()
	as.True(in == 4 || in == 6)
	if in == 4 {
		as.Equal(6, <-c.Receive())
	} else {
		as.Equal(4, <-c.Receive())
	}
	in = <-c.Receive()
	as.True(in == 11 || in == 20)
	if in == 11 {
		as.Equal(20, <-c.Receive())
	} else {
		as.Equal(11, <-c.Receive())
	}
	c.Close()

	as.Nil(s.Stop())
}
