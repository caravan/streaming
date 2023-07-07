package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

func SidechainTo[Msg any](ch chan<- Msg) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) error {

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
				return nil
			} else if !forwardToChannel(msg) {
				return nil
			} else if !c.ForwardResult(msg) {
				return nil
			}
		}
	}
}
