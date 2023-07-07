package stream

import (
	"errors"
	"log"
	"sync"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
)

type (
	// Stream is the internal implementation of a stream.Stream
	Stream[In, Out any] struct {
		root stream.Processor[stream.Source, Out]
	}

	// Running is the internal implementation of a stream.Running
	Running[In, Out any] struct {
		sync.Mutex
		*Stream[In, Out]
		done chan context.Done
	}
)

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
func (s *Stream[In, Out]) Start() stream.Running {
	r := &Running[In, Out]{
		Stream: s,
		done:   make(chan context.Done),
	}
	r.startStream(
		r.startMonitor(),
	)
	return r
}

func (r *Running[_, Out]) startStream(monitor chan context.Advice) {
	go func() {
		in := make(chan stream.Source)
		out := make(chan stream.Sink)

		loop := node.Bind(
			r.root,
			node.Sink[Out](),
		)

		loop.Start(context.Make(r.done, monitor, in, out))

		for {
			select {
			case <-r.done:
				return
			case in <- stream.Source{}:
			}
		}
	}()
}

func (r *Running[_, _]) startMonitor() chan context.Advice {
	monitor := make(chan context.Advice)
	go func() {
		for {
			select {
			case <-r.done:
				return
			case a := <-monitor:
				r.handleAdvice(a)
			}
		}
	}()
	return monitor
}

func (r *Running[_, _]) handleAdvice(a context.Advice) {
	switch e := a.(type) {
	case *context.Error:
		log.Print(e.Error())
	case *context.Fatal:
		log.Print(e.Error())
		_ = r.Stop()
	case context.Stop:
		_ = r.Stop()
	}
}

// IsRunning returns whether the stream is actively running
func (r *Running[_, _]) IsRunning() bool {
	r.Lock()
	defer r.Unlock()
	return r.isRunning()
}

func (r *Running[_, _]) isRunning() bool {
	select {
	case <-r.done:
		return false
	default:
		return true
	}
}

// Stop the stream if it's running
func (r *Running[_, _]) Stop() error {
	r.Lock()
	defer r.Unlock()

	if !r.isRunning() {
		return errors.New(stream.ErrAlreadyStopped)
	}
	r.stop()
	return nil
}

func (r *Running[_, _]) stop() {
	close(r.done)
}
