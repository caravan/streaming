package build

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
)

type (
	// Builder is a fluent interface for creating topologies of Processors
	Builder[Msg any] interface {
		TerminalBuilder[Msg]

		// Merge forwards the events being produced by this Builder along
		// with the ones being produced by the provided Builders
		Merge(...Builder[Msg]) Builder[Msg]

		// Join attempts to join the events being produced by this Builder
		// with another, using the provided Joiner, but only if the
		// BinaryPredicate is satisfied
		Join(
			Builder[Msg], node.BinaryPredicate[Msg], node.Joiner[Msg],
		) Builder[Msg]

		// Filter forwards events being produced by this Builder that
		// match the provided Predicate
		Filter(node.Predicate[Msg]) Builder[Msg]

		// Map forwards events being produced by this Builder that are
		// transformed based on the provided Mapper function
		Map(node.Mapper[Msg]) Builder[Msg]

		// Reduce forwards events from this Builder that are the result
		// of reducing the Events it sees into some form of aggregated
		// Events, based on the provided Reducer
		Reduce(node.Reducer[Msg]) Builder[Msg]

		// ReduceFrom provides the same forwarding of events as Reduce,
		// but uses the provided Event as the starting value for the
		// underlying reduction
		ReduceFrom(node.Reducer[Msg], Msg) Builder[Msg]

		// Processor adds the specified Processor to this Builder
		Processor(stream.Processor[Msg]) Builder[Msg]

		// ProcessorFunc adds the specified ProcessorFunc to this Builder
		ProcessorFunc(stream.ProcessorFunc[Msg]) Builder[Msg]

		// Deferred adds a Deferred Processor instantiator to this
		// Builder. These functions are called when the Builder is finally
		// built or instantiated as a Stream
		Deferred(Deferred[Msg]) Builder[Msg]

		// Sink adds a SinkProcessor to this Builder. Because a
		// SinkProcessor is considered a terminal in the graph, the
		// Builder that is returned only exposes the Build and Stream
		// methods
		Sink(stream.SinkProcessor[Msg]) TerminalBuilder[Msg]

		// TopicSink adds a SinkProcessor to this Builder that is based
		// on the specified Topic. So all events that this Stream produces
		// will end up in that Topic. This is a terminal in the graph
		TopicSink(topic.Topic[Msg]) TerminalBuilder[Msg]
	}

	// TerminalBuilder is a sub-portion of the Builder interface that is
	// used to either generate a Processor (for further building) or a
	// Stream that can be used to activate a topology
	TerminalBuilder[Msg any] interface {
		// Build returns a fully realized Processor based on this Builder
		Build() (stream.Processor[Msg], error)

		// Stream returns a fully realized Stream based on this Builder
		Stream() (stream.Stream, error)
	}

	// Deferred is a function signature that can be used to register a
	// Processor instantiator, to be performed at Build time
	Deferred[Msg any] func() (stream.Processor[Msg], error)
)
