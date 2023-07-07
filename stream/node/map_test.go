package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/stream"
)

func TestMap(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[string]()
	outTopic := essentials.NewTopic[string]()

	s := internal.Make(
		node.TopicConsumer(inTopic),
		node.Subprocess(
			node.Map(func(s string) string {
				return "Hello, " + s + "!"
			}),
			node.TopicProducer(outTopic),
		),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- "Caravan"
	p.Close()

	c := outTopic.NewConsumer()
	greeting := <-c.Receive()
	c.Close()

	as.Equal("Hello, Caravan!", greeting)
	as.Nil(s.Stop())
}
