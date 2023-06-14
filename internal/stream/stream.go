package stream

import (
	"errors"
	"sync"

	"github.com/caravan/streaming/internal/stream/node"
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/stream"
)

type (
	// Stream is the internal implementation of a Stream
	Stream[Msg any] struct {
		sync.Mutex
		root    stream.Processor[Msg]
		running bool
	}

	// Stop is a special Error that instructs the Stream to completely
	// stop operating. This should only be used in exceptional cases
	Stop struct{}
)

// Error messages
const (
	ErrAlreadyStarted = "stream already running"
	ErrAlreadyStopped = "stream already stopped"
	ErrStopRequested  = "stream stop requested"
)

// Make builds a Stream
func Make[Msg any](p ...stream.Processor[Msg]) *Stream[Msg] {
	return &Stream[Msg]{
		root: node.Subprocess(p...),
	}
}

// Start kicks off the background routine for this Stream
func (s *Stream[Msg]) Start() error {
	s.Lock()
	if s.isRunning() {
		s.Unlock()
		return errors.New(ErrAlreadyStarted)
	}
	s.running = true
	s.Unlock()

	r := reporter.Make(
		func(_ Msg) {},
		func(e error) {
			if _, ok := e.(Stop); ok {
				s.Lock()
				s.running = false
				s.Unlock()
			}
		},
	)

	go func() {
		for s.IsRunning() {
			var init Msg
			s.root.Process(init, r)
		}
	}()
	return nil
}

// IsRunning returns whether the Stream is actively running
func (s *Stream[_]) IsRunning() bool {
	s.Lock()
	defer s.Unlock()
	return s.isRunning()
}

func (s *Stream[_]) isRunning() bool {
	return s.running
}

// Stop the Stream if it's running
func (s *Stream[_]) Stop() error {
	s.Lock()
	defer s.Unlock()
	if !s.running {
		return errors.New(ErrAlreadyStopped)
	}
	s.running = false
	return nil
}

func (Stop) Error() string {
	return ErrStopRequested
}
