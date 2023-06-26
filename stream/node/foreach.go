package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// ForEachFunc is the signature for a function that can perform some action on
// the incoming messages of a Stream.
type ForEachFunc[Msg any] func(Msg)

// ForEach constructs a processor that performs an action on the messages it
// sees using the provided function, and then forwards the message
func ForEach[Msg any](fn ForEachFunc[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else {
				fn(msg)
				if !c.ForwardResult(msg) {
					return
				}
			}
		}
	}
}
