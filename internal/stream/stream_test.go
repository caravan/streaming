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
}

func TestStreamStart(t *testing.T) {
	as := assert.New(t)

	s := makeGeneratingStream("hello").Start()
	as.NotNil(s)
	as.Nil(s.Stop())
}

func TestStreamStop(t *testing.T) {
	as := assert.New(t)

	s := makeGeneratingStream("hello").Start()
	as.NotNil(s)

	as.Nil(s.Stop())
	as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamMonitorStop(t *testing.T) {
	as := assert.New(t)
	var r stream.Running
	s := internal.Make[any](
		node.Generate(func() (any, bool) {
			return "hello", true
		}),
		func(c *context.Context[any, any]) error {
			as.True(r.IsRunning())
			c.Advise(context.Stop{})

			return nil
		},
	)
	r = s.Start()
	as.NotNil(r)
	done := make(chan bool)
	go func() {
		time.Sleep(500 * time.Millisecond)
		as.EqualError(r.Stop(), stream.ErrAlreadyStopped)
		done <- true
	}()
	<-done
}
