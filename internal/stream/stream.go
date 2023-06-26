package stream

import (
	"errors"
	"sync"

	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"

	_stream "github.com/caravan/streaming/stream"
)

// stream is the internal implementation of a stream
type stream[Msg, Res any] struct {
	sync.Mutex
	root _stream.Processor[Msg, Res]
	done chan context.Done
}

// ReportError messages
const (
	ErrAlreadyStarted = "stream already running"
	ErrAlreadyStopped = "stream already stopped"
)

// Make builds a Stream. The Stream must be started using the Start method
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
	s.done = make(chan context.Done)
	s.Unlock()

	err := make(chan error)
	go func() {
		for {
			select {
			case <-s.done:
				return
			case e := <-err:
				if _, ok := e.(_stream.Stop); ok {
					s.Lock()
					close(s.done)
					s.Unlock()
				}
			}
		}
	}()

	go func() {
		in := make(chan Msg)
		out := make(chan Res)

		loop := node.Bind(
			s.root,
			node.Sink[Res](),
		)

		loop.Start(context.Make(s.done, err, in, out))

		var zero Msg
		for {
			select {
			case <-s.done:
				return
			case in <- zero:
			}
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
	if s.done == nil {
		return false
	}

	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

// Stop the stream if it's running
func (s *stream[_, _]) Stop() error {
	s.Lock()
	defer s.Unlock()

	if !s.isRunning() {
		return errors.New(ErrAlreadyStopped)
	}
	close(s.done)
	s.done = nil

	return nil
}
