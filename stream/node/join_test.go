package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func joinGreaterThan(l topic.Event, r topic.Event) bool {
	return l.(int) > r.(int)
}

func joinSum(l topic.Event, r topic.Event) topic.Event {
	return l.(int) + r.(int)
}

func makeJoinError(err error) stream.Processor {
	return stream.ProcessorFunc(
		func(_ topic.Event, r stream.Reporter) {
			r.Error(err)
		},
	)
}

func TestJoinSource(t *testing.T) {
	node.Join(nil, nil, nil, nil).Source()
}

func TestJoin(t *testing.T) {
	as := assert.New(t)

	leftTopic := essentials.NewTopic()
	rightTopic := essentials.NewTopic()
	outTopic := essentials.NewTopic()
	s := streaming.NewStream(
		node.Join(
			node.TopicSource(leftTopic),
			node.TopicSource(rightTopic),
			joinGreaterThan, joinSum,
		),
		node.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	lp := leftTopic.NewProducer()
	rp := rightTopic.NewProducer()
	lp.Send(3) // no match
	time.Sleep(10 * time.Millisecond)
	rp.Send(10) // no match
	lp.Send(5)
	time.Sleep(10 * time.Millisecond)
	rp.Send(3)
	rp.Send(4) // no match
	time.Sleep(10 * time.Millisecond)
	lp.Send(3) // no match
	rp.Send(9)
	time.Sleep(10 * time.Millisecond)
	lp.Send(12)
	_ = rp.Close()
	_ = lp.Close()

	c := outTopic.NewConsumer()
	as.Equal(8, topic.MustReceive(c))
	as.Equal(21, topic.MustReceive(c))
	_ = c.Close()

	as.Nil(s.Stop())
}

func TestJoinErrored(t *testing.T) {
	as := assert.New(t)

	inTopic := essentials.NewTopic()
	outTopic := essentials.NewTopic()
	s := streaming.NewStream(
		node.Join(
			node.TopicSource(inTopic),
			makeJoinError(errors.New("error")),
			joinGreaterThan, joinSum,
		),
		node.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send(32)
	_ = p.Close()

	c := outTopic.NewConsumer()
	e, ok := c.Poll(100 * time.Millisecond) // nothing should come out
	as.Nil(e)
	as.False(ok)
	_ = c.Close()

	as.Nil(s.Stop())
}
