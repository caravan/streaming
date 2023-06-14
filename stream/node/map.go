package node

import "github.com/caravan/streaming/stream"

// Mapper is the signature for a function that can perform Stream mapping. The
// Event that is returned will be passed downstream
type Mapper[Msg any] func(Msg) Msg

// Map constructs a processor that maps the Events it sees into new Events
// using the provided function
func Map[Msg any](fn Mapper[Msg]) stream.Processor[Msg] {
	return fn
}

// Process turns Mapper into a stream.Processor
func (fn Mapper[Msg]) Process(m Msg, r stream.Reporter[Msg]) {
	r.Result(fn(m))
}
