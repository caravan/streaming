package node

import "github.com/caravan/streaming/stream/context"

func Forward[Msg any](c *context.Context[Msg, Msg]) {
	for {
		if msg, ok := c.FetchMessage(); !ok {
			return
		} else if !c.ForwardResult(msg) {
			return
		}
	}
}
