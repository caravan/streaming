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
func Reduce[Msg, Res any](
	fn Reducer[Res, Msg],
) stream.Processor[Msg, Res] {
	return reduce(fn, nil)
}

// ReduceFrom constructs a processor that reduces the messages it sees into some
// form of aggregated messages, based on the provided function and an initial
// message
func ReduceFrom[Msg, Res any](
	fn Reducer[Res, Msg], init Res,
) stream.Processor[Msg, Res] {
	return reduce(fn, func() Res {
		return init
	})
}

func reduce[Msg, Res any](
	fn Reducer[Res, Msg], initial initialReduction[Res],
) stream.Processor[Msg, Res] {
	return func(c *context.Context[Msg, Res]) {
		var fetchFirst func() (Res, bool)

		if initial != nil {
			fetchFirst = func() (Res, bool) {
				return initial(), true
			}
		} else {
			fetchFirst = func() (Res, bool) {
				var zero Res
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
