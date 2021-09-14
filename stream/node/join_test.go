package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/receiver"
	"github.com/caravan/essentials/sender"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func joinGreaterThan(l event.Event, r event.Event) bool {
	return l.(int) > r.(int)
}

func joinSum(l event.Event, r event.Event) event.Event {
	return l.(int) + r.(int)
}

func makeJoinError(err error) stream.Processor {
	return stream.ProcessorFunc(
		func(_ event.Event, r stream.Reporter) {
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
	sender.Send(lp, 3) // no match
	time.Sleep(10 * time.Millisecond)
	sender.Send(rp, 10) // no match
	sender.Send(lp, 5)
	time.Sleep(10 * time.Millisecond)
	sender.Send(rp, 3)
	sender.Send(rp, 4) // no match
	time.Sleep(10 * time.Millisecond)
	sender.Send(lp, 3) // no match
	sender.Send(rp, 9)
	time.Sleep(10 * time.Millisecond)
	sender.Send(lp, 12)
	rp.Close()
	lp.Close()

	c := outTopic.NewConsumer()
	as.Equal(8, receiver.MustReceive(c))
	as.Equal(21, receiver.MustReceive(c))
	c.Close()

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
	sender.Send(p, 32)
	p.Close()

	c := outTopic.NewConsumer()
	e, ok := receiver.Poll(c, 100*time.Millisecond) // nothing should come out
	as.Nil(e)
	as.False(ok)
	c.Close()

	as.Nil(s.Stop())
}
