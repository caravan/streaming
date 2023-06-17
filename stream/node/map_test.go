package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[string]()
	outTopic := essentials.NewTopic[string]()
	typed := streaming.Of[string]()
	s := typed.NewStream(
		typed.TopicSource(inTopic),
		typed.Map(func(s string) string {
			return "Hello, " + s + "!"
		}),
		typed.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- "Caravan"
	p.Close()

	c := outTopic.NewConsumer()
	greeting := <-c.Receive()
	c.Close()

	as.Equal("Hello, Caravan!", greeting)
	as.Nil(s.Stop())
}
