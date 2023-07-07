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
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	c := outTopic.NewConsumer()
	testUnorderedIntResults(t, c.Receive(), 4, 6, 11, 20)
	c.Close()

	as.Nil(s.Stop())
}

func testUnorderedIntResults(t *testing.T, c <-chan int, nums ...int) {
	as := assert.New(t)

	choices := map[int]bool{}
	for _, n := range nums {
		choices[n] = true
	}
	for i := 0; i < len(nums); i++ {
		v := <-c
		_, ok := choices[v]
		as.True(ok)
		delete(choices, v)
	}

	as.Zero(len(choices))
}
