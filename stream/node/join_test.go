package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func joinGreaterThan(l message.Event, r message.Event) bool {
	return l.(int) > r.(int)
}

func joinSum(l message.Event, r message.Event) message.Event {
	return l.(int) + r.(int)
}

func makeJoinError(err error) stream.Processor {
	return stream.ProcessorFunc(
		func(_ message.Event, r stream.Reporter) {
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
	message.Send(lp, 3) // no match
	time.Sleep(10 * time.Millisecond)
	message.Send(rp, 10) // no match
	message.Send(lp, 5)
	time.Sleep(10 * time.Millisecond)
	message.Send(rp, 3)
	message.Send(rp, 4) // no match
	time.Sleep(10 * time.Millisecond)
	message.Send(lp, 3) // no match
	message.Send(rp, 9)
	time.Sleep(10 * time.Millisecond)
	message.Send(lp, 12)
	rp.Close()
	lp.Close()

	c := outTopic.NewConsumer()
	as.Equal(8, message.MustReceive(c))
	as.Equal(21, message.MustReceive(c))
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
	message.Send(p, 32)
	p.Close()

	c := outTopic.NewConsumer()
	e, ok := message.Poll(c, 100*time.Millisecond) // nothing should come out
	as.Nil(e)
	as.False(ok)
	c.Close()

	as.Nil(s.Stop())
}
