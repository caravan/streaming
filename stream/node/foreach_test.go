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
	sink := node.ForEach[any](nil).(stream.SinkProcessor[any])
	sink.Sink()
}

func TestForEach(t *testing.T) {
	as := assert.New(t)

	sum := 0
	inTopic := topic.New[int]()
	typed := streaming.Of[int]()
	s := typed.NewStream(
		typed.TopicSource(inTopic),
		typed.ForEach(func(m int) {
			sum += m
		}),
	)

	as.Nil(s.Start())
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3
	p.Close()

	time.Sleep(50 * time.Millisecond)
	as.Equal(6, sum)
	as.Nil(s.Stop())
}
