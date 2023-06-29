package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// Merge forwards results from multiple Processors to the same channel
func Merge[Res any](
	p ...stream.Processor[stream.Source, Res],
) stream.Processor[stream.Source, Res] {
	return func(c *context.Context[stream.Source, Res]) {
		for _, proc := range p {
			proc.Start(c)
		}
		<-c.Done
	}
}
