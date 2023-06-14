package node

import "github.com/caravan/streaming/stream"

type (
	// Predicate is the signature for a function that can perform Stream
	// filtering. Returning false will drop the Event from the Stream
	Predicate[Msg any] func(Msg) bool

	filter[Msg any] Predicate[Msg]
)

// Filter constructs a processor that will only forward to a Result if the
// provided function returns true
func Filter[Msg any](fn Predicate[Msg]) stream.Processor[Msg] {
	return filter[Msg](fn)
}

// Process turns FilterFunc into a stream.Processor
func (fn filter[Msg]) Process(m Msg, r stream.Reporter[Msg]) {
	if fn(m) {
		r.Result(m)
	}
}
