package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

type (
	// BinaryPredicate is the signature for a function that can perform
	// Stream joining. Returning true will bind the messages in the Stream
	BinaryPredicate[Left, Right any] func(Left, Right) bool

	// BinaryOperator combines the left and right messages into some new result
	BinaryOperator[Left, Right, Res any] func(Left, Right) Res
)

// Join accepts two Processors for the sake of joining their results based on a
// provided BinaryPredicate and BinaryOperator. If the predicate fails, nothing
// is forwarded, otherwise the two processed messages are combined using the join
// function, and the result is forwarded
func Join[Left, Right, Res any](
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
	predicate BinaryPredicate[Left, Right],
	joiner BinaryOperator[Left, Right, Res],
) stream.Processor[stream.Source, Res] {
	return func(c *context.Context[stream.Source, Res]) {
		leftOut := make(chan Left)
		rightOut := make(chan Right)
		left.Start(context.Make(c.Done, c.Errors, c.In, leftOut))
		right.Start(context.Make(c.Done, c.Errors, c.In, rightOut))

		joinResults := func() (Left, Right, bool) {
			var leftZero Left
			var rightZero Right
			select {
			case <-c.Done:
				return leftZero, rightZero, false
			case leftMsg := <-leftOut:
				select {
				case <-c.Done:
					return leftZero, rightZero, false
				case rightMsg := <-rightOut:
					return leftMsg, rightMsg, true
				}
			case rightMsg := <-rightOut:
				select {
				case <-c.Done:
					return leftZero, rightZero, false
				case leftMsg := <-leftOut:
					return leftMsg, rightMsg, true
				}
			}
		}

		for {
			if left, right, ok := joinResults(); !ok {
				return
			} else if !predicate(left, right) {
				continue
			} else if !c.ForwardResult(joiner(left, right)) {
				return
			}
		}
	}
}
