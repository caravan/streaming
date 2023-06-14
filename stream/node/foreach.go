package node

import "github.com/caravan/streaming/stream"

// ForEachFunc is the signature for a function that can perform some action on
// the incoming Events of a Stream.
type ForEachFunc[Msg any] func(Msg)

// ForEach constructs a processor that performs an action on the Events it sees
// using the provided function. This type of processor node is considered a
// stream.SinkProcessor, and will not forward results
func ForEach[Msg any](fn ForEachFunc[Msg]) stream.Processor[Msg] {
	return fn
}

// Sink marks ForEachFunc as a stream.SinkProcessor
func (ForEachFunc[_]) Sink() {}

// Process turns ForEachFunc into a stream.Processor
func (fn ForEachFunc[Msg]) Process(m Msg, _ stream.Reporter[Msg]) {
	fn(m)
}
