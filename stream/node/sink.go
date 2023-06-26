package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

func Sink[Msg any]() stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			}
		}
	}
}
