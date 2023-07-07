package stream

import (
	"errors"
	"sync"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
)

// Stream is the internal implementation of a stream
type Stream[In, Out any] struct {
	sync.Mutex
	root stream.Processor[stream.Source, Out]
	done chan context.Done
}

// Make builds a Stream. The Stream must be started using the Start method
func Make[In, Out any](
	source stream.Processor[stream.Source, In],
	rest stream.Processor[In, Out],
) stream.Stream {
	return &Stream[In, Out]{
		root: node.Bind(source, rest),
	}
}

// Start kicks off the background routine for this stream
func (s *Stream[In, Out]) Start() error {
	s.Lock()
	if s.isRunning() {
		s.Unlock()
		return errors.New(stream.ErrAlreadyStarted)
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
				if _, ok := e.(stream.Stop); ok {
					s.Lock()
					close(s.done)
					s.Unlock()
				}
			}
		}
	}()

	go func() {
		in := make(chan stream.Source)
		out := make(chan stream.Sink)

		loop := node.Bind(
			s.root,
			node.Sink[Out](),
		)

		loop.Start(context.Make(s.done, err, in, out))

		for {
			select {
			case <-s.done:
				return
			case in <- stream.Source{}:
			}
		}
	}()
	return nil
}

// IsRunning returns whether the stream is actively running
func (s *Stream[In, Out]) IsRunning() bool {
	s.Lock()
	defer s.Unlock()
	return s.isRunning()
}

func (s *Stream[In, Out]) isRunning() bool {
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
func (s *Stream[In, Out]) Stop() error {
	s.Lock()
	defer s.Unlock()

	if !s.isRunning() {
		return errors.New(stream.ErrAlreadyStopped)
	}
	close(s.done)
	s.done = nil

	return nil
}
