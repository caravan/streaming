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

func SinkInto[Msg any](ch chan Msg) stream.Processor[Msg, stream.Sink] {
	return func(c *context.Context[Msg, stream.Sink]) {
		forwardMsg := func(msg Msg) bool {
			select {
			case <-c.Done:
				return false
			case ch <- msg:
				return true
			}
		}

		for {
			if msg, ok := c.FetchMessage(); ok && forwardMsg(msg) {
				continue
			}
			close(ch)
			return
		}
	}
}
