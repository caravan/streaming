package node_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/debug"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestTopicSourceSink(t *testing.T) {
	top := essentials.NewTopic()
	node.TopicSource(top).Source()
	node.TopicSink(top).Sink()
}

func TestTopicGC(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic()
	node.Subprocess(
		node.TopicSource(top),
		node.TopicSink(top),
	)

	debug.Enable()
	runtime.GC()
	debug.WithConsumer(func(c topic.Consumer) {
		e, ok := message.Poll(c, 200*time.Millisecond)
		as.Nil(e)
		as.False(ok)
	})
}
