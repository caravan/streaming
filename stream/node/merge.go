package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// Merge forwards results from multiple Processors to the same channel
func Merge[Out any](
	p ...stream.Processor[stream.Source, Out],
) stream.Processor[stream.Source, Out] {
	return func(c *context.Context[stream.Source, Out]) {
		for _, proc := range p {
			proc.Start(c)
		}
	}
}
