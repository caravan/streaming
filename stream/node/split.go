package node

import (
	"sync"
	"sync/atomic"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

func Split[In, Out any](
	p ...stream.Processor[In, Out],
) stream.Processor[In, stream.Sink] {
	return func(c *context.Context[In, stream.Sink]) error {
		sink := make(chan Out)
		Sink[Out]().Start(
			context.With(c, sink, make(chan stream.Sink)),
		)

		handoff := make([]chan In, len(p))
		for i, proc := range p {
			ch := make(chan In)
			handoff[i] = ch
			proc.Start(context.With(c, ch, sink))
		}

		forwardInput := func(msg In) bool {
			var isDone atomic.Bool
			var group sync.WaitGroup
			group.Add(len(p))

			for _, ch := range handoff {
				go func(ch chan In) {
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
				return nil
			} else if !forwardInput(msg) {
				return nil
			}
		}
	}
}
