package node_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/stream"
)

func TestForEach(t *testing.T) {
	as := assert.New(t)

	sum := 0
	inTopic := essentials.NewTopic[int]()

	s := internal.Make(
		node.TopicConsumer(inTopic),
		node.ForEach(func(m int) {
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
