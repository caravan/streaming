package stream

func Combine[Msg, Handoff, Res any](
	l Processor[Msg, Handoff],
	r Processor[Handoff, Res],
) Processor[Msg, Res] {
	return func(msg Msg, rep Reporter[Res]) {
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
