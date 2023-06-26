package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// Bind the result of the left Processor to the input of the right Processor,
// returning a new Processor that performs the handoff. If an error is reported
// by the left Processor, the handoff will be short-circuited and the error
// will be reported downstream.
//
// Processor[Msg, Res] = Processor[Msg, Handoff] -> Processor[Handoff, Res]
//
// Msg is the input type of the left Processor, Handoff is the type of the left
// Processor's result, as well as the input type of the right Processor. Res is
// the type of the right Processor's result
func Bind[Msg, Handoff, Res any](
	left stream.Processor[Msg, Handoff],
	right stream.Processor[Handoff, Res],
) stream.Processor[Msg, Res] {
	return func(c *context.Context[Msg, Res]) {
		h := make(chan Handoff)
		left.Start(context.Make(c.Done, c.Errors, c.In, h))
		right.Start(context.Make(c.Done, c.Errors, h, c.Out))
		<-c.Done
	}
}
