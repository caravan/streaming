package node

import (
	"sync"
	"sync/atomic"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// Merge forwards results from multiple Processors to the same channel
func Merge[Msg, Res any](
	p ...stream.Processor[Msg, Res],
) stream.Processor[Msg, Res] {
	return func(c *context.Context[Msg, Res]) {
		handoff := make([]chan Msg, len(p))
		for i, proc := range p {
			ch := make(chan Msg)
			handoff[i] = ch
			proc.Start(c.WithNewIn(ch))
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
