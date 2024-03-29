package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

type Generator[Msg any] func() (Msg, bool)

func Generate[Msg any](
	gen Generator[Msg],
) stream.Processor[stream.Source, Msg] {
	return func(c *context.Context[stream.Source, Msg]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			} else if res, ok := gen(); !ok {
				return
			} else if !c.ForwardResult(res) {
				return
			}
		}
	}
}

func GenerateFrom[Msg any](
	ch <-chan Msg,
) stream.Processor[stream.Source, Msg] {
	return Generate(func() (Msg, bool) {
		msg, ok := <-ch
		return msg, ok
	})
}
