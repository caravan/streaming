package node

import "github.com/caravan/streaming/stream"

// ForEachFunc is the signature for a function that can perform some action on
// the incoming Events of a Stream.
type ForEachFunc func(stream.Event)

// ForEach constructs a processor that performs an action on the Events it sees
// using the provided function. This type of processor node is considered a
// stream.SinkProcessor, and will not forward results
func ForEach(fn ForEachFunc) stream.Processor {
	return fn
}

// Sink marks ForEachFunc as a stream.SinkProcessor
func (ForEachFunc) Sink() {}

// Process turns ForEachFunc into a stream.Processor
func (fn ForEachFunc) Process(e stream.Event, _ stream.Reporter) {
	fn(e)
}
