package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

type (
	// Reducer is the signature for a function that can perform Stream
	// reduction. The message that is returned will be passed downstream
	Reducer[Res, Msg any] func(Res, Msg) Res

	initialReduction[Res any] func() Res
)

// Reduce constructs a processor that reduces the messages it sees into some form
// of aggregated messages, based on the provided function
func Reduce[In, Out any](
	fn Reducer[Out, In],
) stream.Processor[In, Out] {
	return reduce(fn, nil)
}

// ReduceFrom constructs a processor that reduces the messages it sees into some
// form of aggregated messages, based on the provided function and an initial
// message
func ReduceFrom[In, Out any](
	fn Reducer[Out, In], init Out,
) stream.Processor[In, Out] {
	return reduce(fn, func() Out {
		return init
	})
}

func reduce[In, Out any](
	fn Reducer[Out, In], initial initialReduction[Out],
) stream.Processor[In, Out] {
	return func(c *context.Context[In, Out]) {
		var fetchFirst func() (Out, bool)

		if initial != nil {
			fetchFirst = func() (Out, bool) {
				return initial(), true
			}
		} else {
			fetchFirst = func() (Out, bool) {
				var zero Out
				if msg, ok := c.FetchMessage(); !ok {
					return zero, false
				} else {
					return fn(zero, msg), true
				}
			}
		}

		if res, ok := fetchFirst(); !ok {
			return
		} else {
			for {
				if msg, ok := c.FetchMessage(); !ok {
					return
				} else {
					res = fn(res, msg)
					if !c.ForwardResult(res) {
						return
					}
				}
			}
		}
	}
}
