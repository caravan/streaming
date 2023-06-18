package stream

import (
	"errors"
	"sync"

	_stream "github.com/caravan/streaming/stream"
)

// stream is the internal implementation of a stream
type stream[Msg, Res any] struct {
	sync.Mutex
	root    _stream.Processor[Msg, Res]
	running bool
}

// Error messages
const (
	ErrAlreadyStarted = "stream already running"
	ErrAlreadyStopped = "stream already stopped"
)

// Make builds a stream
func Make[Msg, Res any](p _stream.Processor[Msg, Res]) _stream.Stream {
	return &stream[Msg, Res]{
		root: p,
	}
}

// Start kicks off the background routine for this stream
func (s *stream[Msg, Res]) Start() error {
	s.Lock()
	if s.isRunning() {
		s.Unlock()
		return errors.New(ErrAlreadyStarted)
	}
	s.running = true
	s.Unlock()

	r := func(_ Res, e error) {
		if _, ok := e.(_stream.Stop); ok {
			s.Lock()
			s.running = false
			s.Unlock()
		}
	}

	go func() {
		for s.IsRunning() {
			var init Msg
			s.root(init, r)
		}
	}()
	return nil
}

// IsRunning returns whether the stream is actively running
func (s *stream[_, _]) IsRunning() bool {
	s.Lock()
	defer s.Unlock()
	return s.isRunning()
}

func (s *stream[_, _]) isRunning() bool {
	return s.running
}

// Stop the stream if it's running
func (s *stream[_, _]) Stop() error {
	s.Lock()
	defer s.Unlock()
	if !s.running {
		return errors.New(ErrAlreadyStopped)
	}
	s.running = false
	return nil
}
