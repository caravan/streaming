package node

import "github.com/caravan/streaming/stream"

type (
	// Reducer is the signature for a function that can perform Stream
	// reduction. The message that is returned will be passed downstream
	Reducer[Res, Msg any] func(Res, Msg) Res

	// Reset allows a Reducer to be reset to its initial state
	Reset func()
)

// Reduce constructs a processor that reduces the messages it sees into some form
// of aggregated messages, based on the provided function
func Reduce[Msg, Res any](
	fn Reducer[Res, Msg],
) (stream.Processor[Msg, Res], Reset) {
	var res Res
	var reduce func(msg Msg, rep stream.Reporter[Res])

	initReduce := func(msg Msg, _ stream.Reporter[Res]) {
		var zero Res
		res = fn(zero, msg)
		reduce = func(msg Msg, rep stream.Reporter[Res]) {
			res = fn(res, msg)
			Forward(res, rep)
		}
	}
	reduce = initReduce

	reset := func() {
		reduce = initReduce
	}

	return func(msg Msg, rep stream.Reporter[Res]) {
		reduce(msg, rep)
	}, reset
}

// ReduceFrom constructs a processor that reduces the messages it sees into some
// form of aggregated messages, based on the provided function and an initial
// message
func ReduceFrom[Msg, Res any](
	fn Reducer[Res, Msg], init Res,
) (stream.Processor[Msg, Res], Reset) {
	res := init
	reset := func() {
		res = init
	}
	return func(msg Msg, rep stream.Reporter[Res]) {
		res = fn(res, msg)
		Forward(res, rep)
	}, reset
}
