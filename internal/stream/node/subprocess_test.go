package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/stretchr/testify/assert"

	_node "github.com/caravan/streaming/internal/stream/node"
)

func TestSubprocessError(t *testing.T) {
	as := assert.New(t)

	r := reporter.Make[any](nil, func(e error) {
		as.NotNil(e)
		as.EqualError(e, "explosion")
	})

	s := _node.Subprocess[any](
		_node.Forward[any]{},
		stream.ProcessorFunc[any](
			func(_ any, r stream.Reporter[any]) {
				r.Error(errors.New("explosion"))
			},
		),
	)

	s.Process(nil, r)
}

func TestEmptySubprocess(t *testing.T) {
	as := assert.New(t)

	typed := streaming.Of[any]()
	s := typed.Subprocess()
	as.NotNil(s)
	as.Equal(_node.Forward[any]{}, s)
}

func TestSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New[string]()
	outTopic := topic.New[string]()

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

	inTopic := topic.New[int]()
	outTopic := topic.New[int]()
	typed := streaming.Of[int]()

	sub := typed.Subprocess(
		typed.TopicSource(inTopic),
		typed.Reduce(func(l int, r int) int {
			return l + r
		}),
		typed.TopicSink(outTopic),
	).(stream.StatefulProcessor[int])

	s := typed.NewStream(sub)
	s.Start()

	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2

	time.Sleep(50 * time.Millisecond)
	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())

	sub.Reset()
	p.Send() <- 11
	p.Send() <- 12
	p.Close()

	time.Sleep(50 * time.Millisecond)
	as.Equal(23, <-c.Receive())
	c.Close()
}
