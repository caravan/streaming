package build

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"
)

type (
	// Builder is a fluent interface for creating topologies of Processors
	Builder interface {
		TerminalBuilder

		// Merge forwards the events being produced by this Builder along
		// with the ones being produced by the provided Builders
		Merge(...Builder) Builder

		// Join attempts to join the events being produced by this Builder
		// with another, using the provided Joiner, but only if the
		// BinaryPredicate is satisfied
		Join(Builder, node.BinaryPredicate, node.Joiner) Builder

		// Filter forwards events being produced by this Builder that
		// match the provided Predicate
		Filter(node.Predicate) Builder

		// Map forwards events being produced by this Builder that are
		// transformed based on the provided Mapper function
		Map(node.Mapper) Builder

		// Reduce forwards events from this Builder that are the result
		// of reducing the Events it sees into some form of aggregated
		// Events, based on the provided Reducer
		Reduce(node.Reducer) Builder

		// ReduceFrom provides the same forwarding of events as Reduce,
		// but uses the provided Event as the starting value for the
		// underlying reduction
		ReduceFrom(node.Reducer, stream.Event) Builder

		// TableLookup retrieves a Key of the events from this Builder,
		// and uses that key to perform a lookup on the provided Table.
		// If an  Event is retrieved from that table, the value in the
		// specified column is forwarded
		TableLookup(table.Table, table.ColumnName, table.KeySelector) Builder

		// Processor adds the specified Processor to this Builder
		Processor(stream.Processor) Builder

		// ProcessorFunc adds the specified ProcessorFunc to this Builder
		ProcessorFunc(stream.ProcessorFunc) Builder

		// Deferred adds a Deferred Processor instantiator to this
		// Builder. These functions are called when the Builder is finally
		// built or instantiated as a Stream
		Deferred(Deferred) Builder

		// Sink adds a SinkProcessor to this Builder. Because a
		// SinkProcessor is considered a terminal in the graph, the
		// Builder that is returned only exposes the Build and Stream
		// methods
		Sink(stream.SinkProcessor) TerminalBuilder

		// TopicSink adds a SinkProcessor to this Builder that is based
		// on the specified Topic. So all events that this Stream produces
		// will end up in that Topic. This is a terminal in the graph
		TopicSink(topic.Topic[stream.Event]) TerminalBuilder

		// TableSink adds a SinkProcessor to this Builder that is based
		// on the specified Table. So all events that this Stream produces
		// will end up in that Table. This is a terminal in the graph
		TableSink(table.Table) TerminalBuilder
	}

	// TerminalBuilder is a sub-portion of the Builder interface that is
	// used to either generate a Processor (for further building) or a
	// Stream that can be used to activate a topology
	TerminalBuilder interface {
		// Build returns a fully realized Processor based on this Builder
		Build() (stream.Processor, error)

		// Stream returns a fully realized Stream based on this Builder
		Stream() (stream.Stream, error)
	}

	// Deferred is a function signature that can be used to register a
	// Processor instantiator, to be performed at Build time
	Deferred func() (stream.Processor, error)
)
