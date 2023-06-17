package node_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	as := assert.New(t)

	add1 := func(e int, r stream.Reporter[int]) {
		r(e+1, nil)
	}

	times2 := func(e int, r stream.Reporter[int]) {
		// Multiplication is slower
		time.Sleep(50 * time.Millisecond)
		r(e*2, nil)
	}

	inTopic := essentials.NewTopic[int]()
	outTopic := essentials.NewTopic[int]()
	typed := streaming.Of[int]()
	s := typed.NewStream(
		typed.TopicSource(inTopic),
		typed.Merge(add1, times2),
		typed.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	c := outTopic.NewConsumer()
	as.Equal(4, <-c.Receive())
	as.Equal(6, <-c.Receive())
	as.Equal(11, <-c.Receive())
	as.Equal(20, <-c.Receive())
	c.Close()

	as.Nil(s.Stop())
}
