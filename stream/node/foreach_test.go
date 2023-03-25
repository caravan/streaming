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

func TestForEachIsSink(t *testing.T) {
	sink := node.ForEach(nil).(stream.SinkProcessor)
	sink.Sink()
}

func TestForEach(t *testing.T) {
	as := assert.New(t)

	sum := 0
	inTopic := topic.New()
	s := streaming.NewStream(
		node.TopicSource(inTopic),
		node.ForEach(func(e stream.Event) {
			sum += e.(int)
		}),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	topic.Send(p, 1)
	topic.Send(p, 2)
	topic.Send(p, 3)
	p.Close()

	time.Sleep(50 * time.Millisecond)
	as.Equal(6, sum)
	as.Nil(s.Stop())
}
