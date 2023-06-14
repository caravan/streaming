package stream

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
	Reporter[Msg any] interface {
		// Result provided for further Stream processing
		Result(Msg)

		// Error provided for Stream problem reporting
		Error(error)
	}

	// Processor is a value that exposes the ability to be processed as
	// part of a Stream topology
	Processor[Msg any] interface {
		Process(Msg, Reporter[Msg])
	}

	// ProcessorFunc is a function that acts as a Processor node
	ProcessorFunc[Msg any] func(Msg, Reporter[Msg])

	// SourceProcessor marks a Processor as a source for incoming messages
	SourceProcessor[Msg any] interface {
		Processor[Msg]
		Source()
	}

	// StatefulProcessor marks a Processor as maintaining an internal
	// state that would influence the messages it forwards. For example,
	// reducers tend to be stateful
	StatefulProcessor[Msg any] interface {
		Processor[Msg]
		Reset()
	}

	// SinkProcessor marks a Processor as a sink for outgoing messages
	SinkProcessor[Msg any] interface {
		Processor[Msg]
		Sink()
	}
)

// Process makes ProcessorFunc a Processor implementation
func (fn ProcessorFunc[Msg]) Process(m Msg, r Reporter[Msg]) {
	fn(m, r)
}
