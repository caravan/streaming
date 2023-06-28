package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

func Sink[Msg any]() stream.Processor[Msg, stream.Sink] {
	return func(c *context.Context[Msg, stream.Sink]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			}
		}
	}
}
