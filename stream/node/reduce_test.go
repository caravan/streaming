package node_test

import (
	"testing"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func sumReducer(prev stream.Event, e stream.Event) stream.Event {
	return prev.(int) + e.(int)
}

func TestReduce(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New()
	outTopic := topic.New()

	sub := node.Subprocess(
		node.TopicSource(inTopic),
		node.Reduce(sumReducer),
		node.TopicSink(outTopic),
	).(stream.StatefulProcessor)
	s := streaming.NewStream(sub)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	c := outTopic.NewConsumer()
	as.Equal(3, topic.MustReceive(c))
	as.Equal(6, topic.MustReceive(c))

	sub.Reset()
	p.Send() <- 4
	p.Send() <- 5
	as.Equal(9, topic.MustReceive(c))

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)

	inTopic := topic.New()
	outTopic := topic.New()
	sub := node.Subprocess(
		node.TopicSource(inTopic),
		node.ReduceFrom(sumReducer, 5),
		node.TopicSink(outTopic),
	).(stream.StatefulProcessor)
	s := streaming.NewStream(sub)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	c := outTopic.NewConsumer()
	as.Equal(6, topic.MustReceive(c))
	as.Equal(8, topic.MustReceive(c))
	as.Equal(11, topic.MustReceive(c))

	sub.Reset()
	p.Send() <- 4
	as.Equal(9, topic.MustReceive(c))

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}

func TestReducerStateful(t *testing.T) {
	as := assert.New(t)
	r := node.Reduce(sumReducer)
	s, ok := r.(stream.StatefulProcessor)
	as.True(ok)
	s.Reset()
}
