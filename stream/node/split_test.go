package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/stream"
)

func TestSplit(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()

	s := internal.Make(
		node.TopicConsumer(inTopic),
		node.Split(
			node.Bind(
				node.Map(func(i int) int {
					return i + 1
				}),
				node.TopicProducer(outTopic),
			),
			node.Bind(
				node.Map(func(i int) int {
					return i * 2
				}),
				node.TopicProducer(outTopic),
			),
		),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	c := outTopic.NewConsumer()
	testUnorderedIntResults(t, c.Receive(), 4, 6, 11, 20)
	c.Close()

	as.Nil(s.Stop())
}
