package stream_test

import (
	"testing"
	"time"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/stream"
)

func makeGeneratingStream(value any) stream.Stream {
	return internal.Make(
		node.Generate(func() (any, bool) { return value, true }),
		node.Forward[any],
	)
}

func TestStreamCreate(t *testing.T) {
	as := assert.New(t)

	s := makeGeneratingStream("hello")
	as.NotNil(s)
	as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamStart(t *testing.T) {
	as := assert.New(t)

	s := makeGeneratingStream("hello")
	as.Nil(s.Start())
	as.EqualError(s.Start(), stream.ErrAlreadyStarted)
}

func TestStreamStop(t *testing.T) {
	as := assert.New(t)

	s := makeGeneratingStream("hello")
	as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamStartStop(t *testing.T) {
	as := assert.New(t)

	s := makeGeneratingStream("hello")
	as.Nil(s.Start())
	as.EqualError(s.Start(), stream.ErrAlreadyStarted)

	as.Nil(s.Stop())
	as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamError(t *testing.T) {
	as := assert.New(t)
	as.Equal(stream.Stop{}.Error(), stream.ErrStopRequested)
	var s stream.Stream
	s = internal.Make[any](
		node.Generate(func() (any, bool) {
			return "hello", true
		}),
		func(c *context.Context[any, any]) {
			as.True(s.IsRunning())
			c.ReportError(stream.Stop{})
		},
	)
	as.Nil(s.Start())
	done := make(chan bool)
	go func() {
		time.Sleep(500 * time.Millisecond)
		as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
		done <- true
	}()
	<-done
}
