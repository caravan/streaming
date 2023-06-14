package node_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/caravan/streaming"

	"github.com/caravan/essentials/debug"
	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestTopicSourceSink(t *testing.T) {
	top := topic.New[any]()
	node.TopicSource[any](top).Source()
	node.TopicSink[any](top).Sink()
}

func TestTopicGC(t *testing.T) {
	as := assert.New(t)

	top := topic.New[any]()
	typed := streaming.Of[any]()
	typed.Subprocess(
		typed.TopicSource(top),
		typed.TopicSink(top),
	)

	debug.Enable()
	runtime.GC()
	debug.WithConsumer(func(c debug.Consumer) {
		e, ok := message.Poll[error](c, 200*time.Millisecond)
		as.Nil(e)
		as.False(ok)
	})
}
