package node

import "github.com/caravan/streaming/stream"

// Predicate is the signature for a function that can perform Stream
// filtering. Returning false will drop the message from the Stream
type Predicate[Msg any] func(Msg) bool

// Filter constructs a processor that will only forward to a Result if the
// provided function returns true
func Filter[Msg any](fn Predicate[Msg]) stream.Processor[Msg, Msg] {
	return func(msg Msg, rep stream.Reporter[Msg]) {
		if fn(msg) {
			Forward(msg, rep)
		}
	}
}
