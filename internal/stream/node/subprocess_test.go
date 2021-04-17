package node_test

import (
	"errors"
	"testing"
	"time"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	_node "github.com/caravan/streaming/internal/stream/node"
)

func TestSubprocessError(t *testing.T) {
	as := assert.New(t)

	r := reporter.Make(nil, func(e error) {
		as.NotNil(e)
		as.EqualError(e, "explosion")
	})

	s := _node.Subprocess(
		_node.Forward,
		stream.ProcessorFunc(
			func(_ topic.Event, r stream.Reporter) {
				r.Error(errors.New("explosion"))
			},
		),
	)

	s.Process(nil, r)
}

func TestEmptySubprocess(t *testing.T) {
	as := assert.New(t)
	s := _node.Subprocess()
	as.NotNil(s)
	as.Equal(_node.Forward, s)
}

func TestSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic()
	outTopic := caravan.NewTopic()

	sub := _node.Subprocess(
		node.TopicSource(inTopic),
		node.TopicSink(outTopic),
	)

	s := streaming.NewStream(sub)
	s.Start()

	p := inTopic.NewProducer()
	p.Send("hello")
	_ = p.Close()

	c := outTopic.NewConsumer()
	as.Equal("hello", topic.MustReceive(c))
	_ = c.Close()
}

func TestStatefulSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic()
	outTopic := caravan.NewTopic()

	sub := _node.Subprocess(
		node.TopicSource(inTopic),
		node.Reduce(func(l topic.Event, r topic.Event) topic.Event {
			return l.(int) + r.(int)
		}),
		node.TopicSink(outTopic),
	).(stream.StatefulProcessor)

	s := streaming.NewStream(sub)
	s.Start()

	p := inTopic.NewProducer()
	p.Send(1)
	p.Send(2)

	time.Sleep(50 * time.Millisecond)
	c := outTopic.NewConsumer()
	as.Equal(3, topic.MustReceive(c))

	sub.Reset()
	p.Send(11)
	p.Send(12)
	_ = p.Close()

	time.Sleep(50 * time.Millisecond)
	as.Equal(23, topic.MustReceive(c))
	_ = c.Close()
}
