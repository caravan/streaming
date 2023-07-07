package node

import "github.com/caravan/streaming/stream/context"

func Forward[Msg any](c *context.Context[Msg, Msg]) error {
	for {
		if msg, ok := c.FetchMessage(); !ok {
			return nil
		} else if !c.ForwardResult(msg) {
			return nil
		}
	}
}
