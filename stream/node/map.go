package node

import (
	"github.com/caravan/essentials/event"
	"github.com/caravan/streaming/stream"
)

// Mapper is the signature for a function that can perform Stream
// mapping. The Event that is returned will be passed downstream
type Mapper func(event.Event) event.Event

// Map constructs a processor that maps the Events it sees into new Events
// using the provided function
func Map(fn Mapper) stream.Processor {
	return fn
}

// Process turns Mapper into a stream.Processor
func (fn Mapper) Process(e event.Event, r stream.Reporter) {
	r.Result(fn(e))
}
