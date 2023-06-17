package stream

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

	Reporter[Res any] func(Res, error)

	// Processor is a function that is processed as part of a Stream topology
	Processor[Msg, Res any] func(Msg, Reporter[Res])

	// Stop is a special Error that instructs the stream to completely
	// stop operating. This should only be used in exceptional cases
	Stop struct{}
)
