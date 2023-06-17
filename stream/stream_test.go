package stream_test

import (
	"testing"
	"time"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/stretchr/testify/assert"
)

func TestStreamCreate(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.NotNil(s)
	as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamStart(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.Nil(s.Start())
	as.EqualError(s.Start(), stream.ErrAlreadyStarted)
}

func TestStreamStop(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamStartStop(t *testing.T) {
	as := assert.New(t)

	s := streaming.NewStream[any]()
	as.Nil(s.Start())
	as.EqualError(s.Start(), stream.ErrAlreadyStarted)

	as.Nil(s.Stop())
	as.EqualError(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamError(t *testing.T) {
	as := assert.New(t)
	as.Equal(stream.Stop{}.Error(), stream.ErrStopRequested)
	s := streaming.NewStream[any](
		func(_ any, r stream.Reporter[any]) {
			r(nil, stream.Stop{})
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
