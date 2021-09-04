package stream

import "github.com/caravan/essentials/topic"

type (
	// Stream is a process that performs the work assigned to it using the
	// set of Processors provided to it when constructed
	Stream interface {
		// Start begins background processing of the Stream
		Start() error

		// Stop instructs the Stream to stop processing as soon as the
		// current Event has completed
		Stop() error

		// IsRunning returns whether the Stream is processing Events in the
		// background
		IsRunning() bool
	}

	// Reporter is used by a Processor to inform further Stream processing
	Reporter interface {
		// Result provided for further Stream processing
		Result(topic.Event)

		// Error provided for Stream problem reporting
		Error(error)
	}

	// Processor is a value that exposes the ability to be processed as
	// part of a Stream topology
	Processor interface {
		Process(topic.Event, Reporter)
	}

	// ProcessorFunc is a function that acts as a Processor node
	ProcessorFunc func(topic.Event, Reporter)

	// SourceProcessor marks a Processor as a source for incoming Events
	SourceProcessor interface {
		Processor
		Source()
	}

	// StatefulProcessor marks a Processor as maintaining an internal
	// state that would influence the Events it forwards. For example,
	// reducers tend to be stateful
	StatefulProcessor interface {
		Processor
		Reset()
	}

	// SinkProcessor marks a Processor as a sink for outgoing Events
	SinkProcessor interface {
		Processor
		Sink()
	}
)

// Process makes ProcessorFunc a Processor implementation
func (fn ProcessorFunc) Process(e topic.Event, r Reporter) {
	fn(e, r)
}
