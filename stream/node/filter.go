package node

import "github.com/caravan/streaming/stream"

type (
	// Predicate is the signature for a function that can perform Stream
	// filtering. Returning false will drop the Event from the Stream
	Predicate func(stream.Event) bool

	filter Predicate
)

// Filter constructs a processor that will only forward to a Result if the
// provided function returns true
func Filter(fn Predicate) stream.Processor {
	return filter(fn)
}

// Process turns FilterFunc into a stream.Processor
func (fn filter) Process(e stream.Event, r stream.Reporter) {
	if fn(e) {
		r.Result(e)
	}
}
