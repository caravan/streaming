package node_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/stream"
)

func TestEmptySubprocess(t *testing.T) {
	as := assert.New(t)

	s := node.Subprocess[any]()
	as.NotNil(s)

	done := make(chan context.Done)
	in := make(chan any)
	out := make(chan any)

	s.Start(context.Make(done, make(chan context.Advice), in, out))
	in <- "hello"
	as.Equal("hello", <-out)
	close(done)
}

func TestSingleSubprocess(t *testing.T) {
	as := assert.New(t)

	double := node.Subprocess[int](func(c *context.Context[int, int]) {
		c.Out <- (<-c.In) * 2
	})

	as.NotNil(double)

	done := make(chan context.Done)
	in := make(chan int)
	out := make(chan int)

	double.Start(context.Make(done, make(chan context.Advice), in, out))
	in <- 8
	as.Equal(16, <-out)
	close(done)
}

func TestDoubleSubprocess(t *testing.T) {
	as := assert.New(t)

	s := node.Subprocess[any](
		node.Forward[any],
		func(c *context.Context[any, any]) {
			<-c.In
			c.Errorf("explosion")
			<-c.Done
		},
	)

	done := make(chan context.Done)
	monitor := make(chan context.Advice)
	in := make(chan any)

	s.Start(context.Make(done, monitor, in, make(chan any)))
	in <- "anything"
	as.EqualError((<-monitor).(error), "explosion")
	close(done)
}

func TestSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[string]()
	outTopic := essentials.NewTopic[string]()

	s := internal.Make(
		node.TopicConsumer(inTopic),
		node.TopicProducer(outTopic),
	).Start()

	as.NotNil(s)
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

	internal.Make(
		node.TopicConsumer(inTopic),
		node.Subprocess(
			node.Forward[int],
			node.Reduce(func(l int, r int) int {
				return l + r
			}),
			node.TopicProducer(outTopic),
		),
	).Start()

	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2

	time.Sleep(50 * time.Millisecond)
	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())

	p.Close()
	c.Close()
}
