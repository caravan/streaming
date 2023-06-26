package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/stretchr/testify/assert"
)

func joinGreaterThan(l int, r int) bool {
	return l > r
}

func joinSum(l int, r int) int {
	return l + r
}

func makeJoinError(e error) stream.Processor[int, int] {
	return func(c *context.Context[int, int]) {
		<-c.In
		c.Errors <- e
	}
}

func TestJoin(t *testing.T) {
	as := assert.New(t)

	leftTopic := essentials.NewTopic[int]()
	rightTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()
	typed := streaming.Of[int]()
	s := typed.NewStream(
		typed.Join(
			typed.TopicConsumer(leftTopic),
			typed.TopicConsumer(rightTopic),
			joinGreaterThan, joinSum,
		),
		typed.TopicProducer(outTopic),
	)

	as.Nil(s.Start())
	lp := leftTopic.NewProducer()
	rp := rightTopic.NewProducer()
	lp.Send() <- 3 // no match
	time.Sleep(10 * time.Millisecond)
	rp.Send() <- 10 // no match
	lp.Send() <- 5
	time.Sleep(10 * time.Millisecond)
	rp.Send() <- 3
	rp.Send() <- 4 // no match
	time.Sleep(10 * time.Millisecond)
	lp.Send() <- 3 // no match
	rp.Send() <- 9
	time.Sleep(10 * time.Millisecond)
	lp.Send() <- 12
	rp.Close()
	lp.Close()

	c := outTopic.NewConsumer() // Otherwise it's discarded
	as.Equal(8, <-c.Receive())
	as.Equal(21, <-c.Receive())
	c.Close()

	as.Nil(s.Stop())
}

func TestJoinErrored(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()
	typed := streaming.Of[int]()
	s := typed.NewStream(
		typed.Join(
			typed.TopicConsumer(inTopic),
			makeJoinError(errors.New("error")),
			joinGreaterThan, joinSum,
		),
		typed.TopicProducer(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 32
	p.Close()

	c := outTopic.NewConsumer()
	e, ok := message.Poll[int](c, 100*time.Millisecond) // nothing should come out
	as.Zero(e)
	as.False(ok)
	c.Close()

	as.Nil(s.Stop())
}
