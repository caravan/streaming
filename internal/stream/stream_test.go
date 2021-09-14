package stream_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials/event"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/stretchr/testify/assert"

	_stream "github.com/caravan/streaming/internal/stream"
)

func TestStreamCreate(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream()
	as.NotNil(s)
	as.EqualError(s.Stop(), _stream.ErrAlreadyStopped)
}

func TestStreamStart(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream()
	as.Nil(s.Start())
	as.EqualError(s.Start(), _stream.ErrAlreadyStarted)
}

func TestStreamStop(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream()
	as.EqualError(s.Stop(), _stream.ErrAlreadyStopped)
}

func TestStreamStartStop(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream()
	as.Nil(s.Start())
	as.EqualError(s.Start(), _stream.ErrAlreadyStarted)

	as.Nil(s.Stop())
	as.EqualError(s.Stop(), _stream.ErrAlreadyStopped)
}

func TestStreamError(t *testing.T) {
	as := assert.New(t)
	as.Equal(_stream.Stop{}.Error(), _stream.ErrStopRequested)
	s := streaming.NewStream(
		stream.ProcessorFunc(
			func(_ event.Event, r stream.Reporter) {
				r.Error(_stream.Stop{})
			},
		),
	)
	as.Nil(s.Start())
	done := make(chan bool)
	go func() {
		time.Sleep(500 * time.Millisecond)
		as.EqualError(s.Stop(), _stream.ErrAlreadyStopped)
		done <- true
	}()
	<-done
}
