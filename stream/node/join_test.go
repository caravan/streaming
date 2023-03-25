package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func joinGreaterThan(l stream.Event, r stream.Event) bool {
	return l.(int) > r.(int)
}

func joinSum(l stream.Event, r stream.Event) stream.Event {
	return l.(int) + r.(int)
}

func makeJoinError(err error) stream.Processor {
	return stream.ProcessorFunc(
		func(_ stream.Event, r stream.Reporter) {
			r.Error(err)
		},
	)
}

func TestJoinSource(t *testing.T) {
	node.Join(nil, nil, nil, nil).Source()
}

func TestJoin(t *testing.T) {
	as := assert.New(t)

	leftTopic := topic.New()
	rightTopic := topic.New()
	outTopic := topic.New()
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
	topic.Send(lp, 3) // no match
	time.Sleep(10 * time.Millisecond)
	topic.Send(rp, 10) // no match
	topic.Send(lp, 5)
	time.Sleep(10 * time.Millisecond)
	topic.Send(rp, 3)
	topic.Send(rp, 4) // no match
	time.Sleep(10 * time.Millisecond)
	topic.Send(lp, 3) // no match
	topic.Send(rp, 9)
	time.Sleep(10 * time.Millisecond)
	topic.Send(lp, 12)
	rp.Close()
	lp.Close()

	c := outTopic.NewConsumer()
	as.Equal(8, topic.MustReceive(c))
	as.Equal(21, topic.MustReceive(c))
	c.Close()

	as.Nil(s.Stop())
}

func TestJoinErrored(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New()
	outTopic := topic.New()
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
	topic.Send(p, 32)
	p.Close()

	c := outTopic.NewConsumer()
	e, ok := topic.Poll(c, 100*time.Millisecond) // nothing should come out
	as.Nil(e)
	as.False(ok)
	c.Close()

	as.Nil(s.Stop())
}
