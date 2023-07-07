package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// Predicate is the signature for a function that can perform Stream
// filtering. Returning false will drop the message from the Stream
type Predicate[Msg any] func(Msg) bool

// Filter constructs a Processor that will only forward its messages if the
// provided function returns true
func Filter[Msg any](fn Predicate[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) error {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return nil
			} else if !fn(msg) {
				continue
			} else if !c.ForwardResult(msg) {
				return nil
			}
		}
	}
}
