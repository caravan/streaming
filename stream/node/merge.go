package node

import (
	"sync"

	"github.com/caravan/streaming/stream"
)

// Merge forwards Events from multiple Processors to the same Reporter
func Merge[Msg, Res any](
	p ...stream.Processor[Msg, Res],
) stream.Processor[Msg, Res] {
	return func(msg Msg, rep stream.Reporter[Res]) {
		var group sync.WaitGroup
		group.Add(len(p))

		for _, fn := range p {
			go func(fn stream.Processor[Msg, Res]) {
				fn(msg, rep)
				group.Done()
			}(fn)
		}

		group.Wait()
	}
}
