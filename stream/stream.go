package stream

import "github.com/caravan/streaming/stream/context"

type (
	// Stream is a process that performs the work assigned to it using the set
	// of Processors provided to it when constructed
	Stream interface {
		// Start begins background processing of the Stream
		Start() Running
	}

	Running interface {
		// Stop instructs the Stream to stop processing
		Stop() error

		// IsRunning returns whether the Stream is processing messages
		IsRunning() bool
	}

	// Processor is a function that processes part of a Stream topology. If an
	// error is returned from a Processor, it is considered a fatal condition
	// for the Stream, which will be immediately stopped. Recoverable errors
	// can be sent to the context.Context's Monitor channel.
	Processor[In, Out any] func(*context.Context[In, Out]) error

	// Source messages are provided to a Processor that is meant to generate
	// messages from a source outside its current Stream. Examples would be
	// node.TopicConsumer and node.Generate
	Source struct{}

	// Sink messages are produced by a Processor that is meant to terminate a
	// Stream. Examples would be node.SinkTo and node.Sink
	Sink struct{}
)

// Errorf messages
const (
	ErrAlreadyStopped    = "stream already stopped"
	ErrProcessorReturned = "processor returned before context closed"
)

// Start begins the Processor in a new go routine, logging any abnormalities
func (p Processor[In, Out]) Start(c *context.Context[In, Out]) {
	go func() {
		if err := p(c); err != nil {
			c.Fatal(err)
			return
		}
		if !c.IsDone() {
			c.Fatalf(ErrProcessorReturned)
		}
	}()
}
