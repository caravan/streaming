package node

import (
	"sync"
	"sync/atomic"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

func Split[Msg, Res any](
	p ...stream.Processor[Msg, Res],
) stream.Processor[Msg, stream.Sink] {
	return func(c *context.Context[Msg, stream.Sink]) {
		sink := make(chan Res)
		Sink[Res]().Start(
			context.Make(c.Done, c.Errors, sink, make(chan stream.Sink)),
		)

		handoff := make([]chan Msg, len(p))
		for i, proc := range p {
			ch := make(chan Msg)
			handoff[i] = ch
			proc.Start(context.Make(c.Done, c.Errors, ch, sink))
		}

		forwardInput := func(msg Msg) bool {
			var isDone atomic.Bool
			var group sync.WaitGroup
			group.Add(len(p))

			for _, ch := range handoff {
				go func(ch chan Msg) {
					defer group.Done()
					select {
					case <-c.Done:
						isDone.Store(true)
					case ch <- msg:
					}
				}(ch)
			}

			group.Wait()
			return !isDone.Load()
		}

		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if !forwardInput(msg) {
				return
			}
		}
	}
}
