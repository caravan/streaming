package stream

import (
	"log"

	"github.com/caravan/streaming/stream/context"
)

type (
	// Stream is a process that performs the work assigned to it using the
	// set of Processors provided to it when constructed
	Stream interface {
		// Start begins background processing of the Stream
		Start() error

		// Stop instructs the Stream to stop processing as soon as the
		// current message has completed
		Stop() error

		// IsRunning returns whether the Stream is processing messages in the
		// background
		IsRunning() bool
	}

	// Processor is a function that processes part of a Stream topology
	Processor[Msg, Res any] func(*context.Context[Msg, Res])

	Source struct{}

	Sink struct{}

	// Stop is a special Error that instructs the stream to completely
	// stop operating. This should only be used in exceptional cases
	Stop struct{}
)

// Error messages
const (
	ErrAlreadyStarted = "stream already running"
	ErrAlreadyStopped = "stream already stopped"
	ErrStopRequested  = "stream stop requested"
)

func (Stop) Error() string {
	return ErrStopRequested
}

// Start begins the Processor in a new go routine, logging any abnormalities
func (p Processor[Msg, Res]) Start(c *context.Context[Msg, Res]) {
	go func() {
		p(c)
		if !c.IsDone() {
			log.Println("processor returned before context closed")
		}
	}()
}
