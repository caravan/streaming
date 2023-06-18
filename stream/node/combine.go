package node

import "github.com/caravan/streaming/stream"

func Combine[Msg, Handoff, Res any](
	l stream.Processor[Msg, Handoff],
	r stream.Processor[Handoff, Res],
) stream.Processor[Msg, Res] {
	return func(msg Msg, rep stream.Reporter[Res]) {
		handoffReporter := func(h Handoff, err error) {
			if err != nil {
				var zero Res
				rep(zero, err)
				return
			}
			r(h, rep)
		}

		l(msg, handoffReporter)
	}
}
