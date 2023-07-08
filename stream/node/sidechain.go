package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

func SidechainTo[Msg any](ch chan<- Msg) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {

		forwardToChannel := func(msg Msg) bool {
			select {
			case <-c.Done:
				return false
			case ch <- msg:
				return true
			}
		}

		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if !forwardToChannel(msg) {
				return
			} else if !c.ForwardResult(msg) {
				return
			}
		}
	}
}
