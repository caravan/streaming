package stream

import (
	"errors"
	"sync"

	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming/internal/stream/node"
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/stream"
)

type (
	// Stream is the internal implementation of a Stream
	Stream struct {
		sync.Mutex
		root    stream.Processor
		running bool
	}

	// Stop is a special Error that instructs the Stream to completely
	// stop operating. This should only be used in exceptional cases
	Stop struct{}

	// Init is a special Event that a Stream provides to the first
	// processor node that it processes. This node is usually assumed to
	// be a SourceProcessor
	Init struct{}
)

// Error messages
const (
	ErrAlreadyStarted = "stream already running"
	ErrAlreadyStopped = "stream already stopped"
	ErrStopRequested  = "stream stop requested"
)

// Make builds a Stream
func Make(p ...stream.Processor) *Stream {
	return &Stream{
		root: node.Subprocess(p...),
	}
}

// Start kicks off the background routine for this Stream
func (s *Stream) Start() error {
	s.Lock()
	if s.isRunning() {
		s.Unlock()
		return errors.New(ErrAlreadyStarted)
	}
	s.running = true
	s.Unlock()

	r := reporter.Make(
		func(_ message.Event) {},
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
			s.root.Process(Init{}, r)
		}
	}()
	return nil
}

// IsRunning returns whether or not the Stream is actively running
func (s *Stream) IsRunning() bool {
	s.Lock()
	defer s.Unlock()
	return s.isRunning()
}

func (s *Stream) isRunning() bool {
	return s.running
}

// Stop the Stream if it's running
func (s *Stream) Stop() error {
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
