package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/stream"
)

func TestMerge(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()

	s := internal.Make(
		node.Merge(
			node.Bind(
				node.TopicConsumer(inTopic),
				node.Map(func(i int) int {
					return i + 1
				}),
			),
			node.Bind(
				node.TopicConsumer(inTopic),
				node.Map(func(i int) int {
					return i * 2
				}),
			),
		),
		node.TopicProducer(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	results := map[int]bool{4: true, 6: true, 11: true, 20: true}

	c := outTopic.NewConsumer()

	for i := 0; i < 4; i++ {
		v := <-c.Receive()
		_, ok := results[v]
		as.True(ok)
		delete(results, v)
	}

	c.Close()

	as.Nil(s.Stop())
}
