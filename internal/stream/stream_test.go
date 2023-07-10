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
	var s stream.Running
	s = internal.Make[any](
		node.Generate(func() (any, bool) {
			return "hello", true
		}),
		func(c *context.Context[any, any]) {
			as.True(s.IsRunning())
			c.Advise(context.Stop{})
		},
	).Start()

	as.NotNil(s)
	done := make(chan bool)
	go func() {
		time.Sleep(50 * time.Millisecond)
		as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
		done <- true
	}()
	<-done
}

func TestStreamRecoverableError(t *testing.T) {
	as := assert.New(t)
	var s stream.Running
	s = internal.Make[any](
		node.Generate(func() (any, bool) {
			return "hello", true
		}),
		func(c *context.Context[any, any]) {
			for {
				select {
				case <-c.Done:
					return
				case <-c.In:
					as.True(s.IsRunning())
					c.Errorf("recoverable")
					time.Sleep(100 * time.Millisecond)
				}
			}
		},
	).StartWith(func(a context.Advice, next func()) {
		e, ok := a.(*context.Error)
		as.True(ok)
		as.EqualError(e, "recoverable")
		next()
	})

	as.NotNil(s)
	done := make(chan bool)
	go func() {
		time.Sleep(50 * time.Millisecond)
		as.True(s.IsRunning())
		_ = s.Stop()
		done <- true
	}()
	<-done
}

func TestStreamFatalError(t *testing.T) {
	as := assert.New(t)
	var s stream.Running
	s = internal.Make[any](
		node.Generate(func() (any, bool) {
			return "hello", true
		}),
		func(c *context.Context[any, any]) {
			as.True(s.IsRunning())
			c.Fatalf("boom")
		},
	).Start()

	as.NotNil(s)
	done := make(chan bool)
	go func() {
		time.Sleep(50 * time.Millisecond)
		as.False(s.IsRunning())
		done <- true
	}()
	<-done
}
