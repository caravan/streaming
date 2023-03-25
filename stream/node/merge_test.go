package node_test

import (
	"testing"
	"time"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestMergeSource(t *testing.T) {
	node.Merge().Source()
}

func TestMerge(t *testing.T) {
	as := assert.New(t)

	add1 := stream.ProcessorFunc(func(e stream.Event, r stream.Reporter) {
		r.Result(e.(int) + 1)
	})

	times2 := stream.ProcessorFunc(func(e stream.Event, r stream.Reporter) {
		// Multiplication is slower
		time.Sleep(50 * time.Millisecond)
		r.Result(e.(int) * 2)
	})

	inTopic := topic.New()
	outTopic := topic.New()
	s := streaming.NewStream(
		node.TopicSource(inTopic),
		node.Merge(add1, times2),
		node.TopicSink(outTopic),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	c := outTopic.NewConsumer()
	as.Equal(4, topic.MustReceive(c))
	as.Equal(6, topic.MustReceive(c))
	as.Equal(11, topic.MustReceive(c))
	as.Equal(20, topic.MustReceive(c))
	c.Close()

	as.Nil(s.Stop())
}
