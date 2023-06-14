package node_test

import (
	"testing"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New[string]()
	outTopic := topic.New[string]()
	typed := streaming.Of[string]()
	s := typed.NewStream(
		typed.TopicSource(inTopic),
		typed.Map(func(e string) string {
			return "Hello, " + e + "!"
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
