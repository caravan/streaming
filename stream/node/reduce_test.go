package node_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming"
	"github.com/stretchr/testify/assert"
)

func sumReducer(prev int, e int) int {
	return prev + e
}

func TestReduce(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()

	typed := streaming.Of[int]()
	sub := typed.Subprocess(
		typed.TopicConsumer(inTopic),
		typed.Reduce(sumReducer),
		typed.TopicProducer(outTopic),
	)
	s := typed.NewStream(sub)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())
	as.Equal(6, <-c.Receive())

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()
	typed := streaming.Of[int]()
	sub := typed.Subprocess(
		typed.TopicConsumer(inTopic),
		typed.ReduceFrom(sumReducer, 5),
		typed.TopicProducer(outTopic),
	)
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

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}
