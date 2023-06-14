package stream_test

import (
	"testing"
	"time"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/stretchr/testify/assert"

	_stream "github.com/caravan/streaming/internal/stream"
)

func TestStreamCreate(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.NotNil(s)
	as.EqualError(s.Stop(), _stream.ErrAlreadyStopped)
}

func TestStreamStart(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.Nil(s.Start())
	as.EqualError(s.Start(), _stream.ErrAlreadyStarted)
}

func TestStreamStop(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.EqualError(s.Stop(), _stream.ErrAlreadyStopped)
}

func TestStreamStartStop(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.Nil(s.Start())
	as.EqualError(s.Start(), _stream.ErrAlreadyStarted)

	as.Nil(s.Stop())
	as.EqualError(s.Stop(), _stream.ErrAlreadyStopped)
}

func TestStreamError(t *testing.T) {
	as := assert.New(t)
	as.Equal(_stream.Stop{}.Error(), _stream.ErrStopRequested)
	s := streaming.NewStream[any](
		stream.ProcessorFunc[any](
			func(_ any, r stream.Reporter[any]) {
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
