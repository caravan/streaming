package node_test

import (
	"testing"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func sumReducer(prev int, e int) int {
	return prev + e
}

func TestReduce(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New[int]()
	outTopic := topic.New[int]()

	typed := streaming.Of[int]()
	sub := typed.Subprocess(
		typed.TopicSource(inTopic),
		typed.Reduce(sumReducer),
		typed.TopicSink(outTopic),
	).(stream.StatefulProcessor[int])
	s := typed.NewStream(sub)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())
	as.Equal(6, <-c.Receive())

	sub.Reset()
	p.Send() <- 4
	p.Send() <- 5
	as.Equal(9, <-c.Receive())

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New[int]()
	outTopic := topic.New[int]()
	typed := streaming.Of[int]()
	sub := typed.Subprocess(
		typed.TopicSource(inTopic),
		typed.ReduceFrom(sumReducer, 5),
		typed.TopicSink(outTopic),
	).(stream.StatefulProcessor[int])
	s := typed.NewStream(sub)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	c := outTopic.NewConsumer()
	as.Equal(6, <-c.Receive())
	as.Equal(8, <-c.Receive())
	as.Equal(11, <-c.Receive())

	sub.Reset()
	p.Send() <- 4
	as.Equal(9, <-c.Receive())

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}

func TestReducerStateful(t *testing.T) {
	as := assert.New(t)
	r := node.Reduce[int](sumReducer)
	s, ok := r.(stream.StatefulProcessor[int])
	as.True(ok)
	s.Reset()
}
