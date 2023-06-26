package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

type Generator[Msg any] func() Msg

func Generate[Msg any](gen Generator[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			} else if !c.ForwardResult(gen()) {
				return
			}
		}
	}
}
