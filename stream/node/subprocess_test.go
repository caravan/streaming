package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestSubprocessError(t *testing.T) {
	as := assert.New(t)

	s := node.Subprocess[any](
		node.Forward[any],
		func(c *context.Context[any, any]) {
			<-c.In
			c.Errors <- errors.New("explosion")
		},
	)

	done := make(chan context.Done)
	err := make(chan error)
	in := make(chan any)

	s.Start(context.Make(done, err, in, make(chan any)))
	in <- "anything"
	as.EqualError(<-err, "explosion")
	close(done)
}

func TestEmptySubprocess(t *testing.T) {
	as := assert.New(t)

	typed := streaming.Of[any]()
	s := typed.Subprocess()
	as.NotNil(s)

	done := make(chan context.Done)
	in := make(chan any)
	out := make(chan any)

	s.Start(context.Make(done, make(chan error), in, out))
	in <- "hello"
	as.Equal("hello", <-out)
	close(done)
}

func TestSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[string]()
	outTopic := essentials.NewTopic[string]()

	typed := streaming.Of[string]()
	sub := typed.Subprocess(
		typed.TopicConsumer(inTopic),
		typed.TopicProducer(outTopic),
	)

	s := typed.NewStream(sub)
	_ = s.Start()

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

	sub := typed.Subprocess(
		typed.TopicConsumer(inTopic),
		typed.Reduce(func(l int, r int) int {
			return l + r
		}),
		typed.TopicProducer(outTopic),
	)

	s := typed.NewStream(sub)
	_ = s.Start()

	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2

	time.Sleep(50 * time.Millisecond)
	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())

	p.Close()
	c.Close()
}
