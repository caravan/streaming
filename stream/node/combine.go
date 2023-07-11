package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// Bind the output of the left Processor to the input of the right Processor,
// returning a new Processor that performs the handoff. If an error is reported
// by the left Processor, the handoff will be short-circuited and the error
// will be reported downstream.
//
// Processor[In, Out] = Processor[In, Bound] -> Processor[Bound, Out]
//
// In is the input type of the left Processor, Bound is the type of the left
// Processor's output as well as the input type of the right Processor. Out is
// the type of the right Processor's output
func Bind[In, Bound, Out any](
	left stream.Processor[In, Bound],
	right stream.Processor[Bound, Out],
) stream.Processor[In, Out] {
	return func(c *context.Context[In, Out]) {
		h := make(chan Bound)
		left.Start(context.WithOut(c, h))
		right.Start(context.WithIn(c, h))
	}
}
