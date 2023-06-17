package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestSubprocessError(t *testing.T) {
	as := assert.New(t)

	s := node.Subprocess[any](
		node.Forward[any],
		func(_ any, rep stream.Reporter[any]) {
			rep(nil, errors.New("explosion"))
		},
	)

	s(nil, func(msg any, err error) {
		as.Nil(msg)
		as.NotNil(err)
		as.EqualError(err, "explosion")
	})
}

func TestEmptySubprocess(t *testing.T) {
	as := assert.New(t)

	typed := streaming.Of[any]()
	s := typed.Subprocess()
	as.NotNil(s)
	s("hello", func(msg any, err error) {
		as.Equal("hello", msg)
		as.Nil(err)
	})
}

func TestSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[string]()
	outTopic := essentials.NewTopic[string]()

	typed := streaming.Of[string]()
	sub := typed.Subprocess(
		typed.TopicSource(inTopic),
		typed.TopicSink(outTopic),
	)

	s := typed.NewStream(sub)
	s.Start()

	p := inTopic.NewProducer()
	p.Send() <- "hello"
	p.Close()

	c := outTopic.NewConsumer()
	as.Equal("hello", <-c.Receive())
	c.Close()
}

func TestStatefulSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()
	typed := streaming.Of[int]()
	reduce, reset := typed.ReduceWithReset(func(l int, r int) int {
		return l + r
	})

	sub := typed.Subprocess(
		typed.TopicSource(inTopic),
		reduce,
		typed.TopicSink(outTopic),
	)

	s := typed.NewStream(sub)
	s.Start()

	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2

	time.Sleep(50 * time.Millisecond)
	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())

	reset()
	p.Send() <- 11
	p.Send() <- 12
	p.Close()

	time.Sleep(50 * time.Millisecond)
	as.Equal(23, <-c.Receive())
	c.Close()
}
