package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// Mapper is the signature for a function that can perform Stream mapping. The
// message that is returned will be passed downstream
type Mapper[From, To any] func(From) To

// Map constructs a processor that maps the messages it sees into new messages
// using the provided function
func Map[From, To any](fn Mapper[From, To]) stream.Processor[From, To] {
	return func(c *context.Context[From, To]) error {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return nil
			} else if !c.ForwardResult(fn(msg)) {
				return nil
			}
		}
	}
}
