package node

import "github.com/caravan/streaming/stream"

type (
	// Reducer is the signature for a function that can perform Stream
	// reduction. The Event that is returned will be passed downstream
	Reducer[Msg any] func(Msg, Msg) Msg

	reduce[Msg any] struct {
		fn   Reducer[Msg]
		prev Msg
		rest bool
	}

	reduceFrom[Msg any] struct {
		fn   Reducer[Msg]
		init Msg
		prev Msg
	}
)

// Reduce constructs a processor that reduces the Events it sees into some form
// of aggregated Events, based on the provided function
func Reduce[Msg any](fn Reducer[Msg]) stream.Processor[Msg] {
	return &reduce[Msg]{
		fn: fn,
	}
}

// ReduceFrom constructs a processor that reduces the Events it sees into some
// form of aggregated Events, based on the provided function and an initial
// Event
func ReduceFrom[Msg any](fn Reducer[Msg], init Msg) stream.Processor[Msg] {
	return &reduceFrom[Msg]{
		fn:   fn,
		init: init,
		prev: init,
	}
}

func (r *reduce[Msg]) Process(m Msg, rep stream.Reporter[Msg]) {
	if !r.rest {
		r.rest = true
		r.prev = m
		return
	}
	r.prev = r.fn(r.prev, m)
	rep.Result(r.prev)
}

func (r *reduce[Msg]) Reset() {
	var nilMsg Msg
	r.prev = nilMsg
	r.rest = false
}

func (r *reduceFrom[Msg]) Process(m Msg, rep stream.Reporter[Msg]) {
	r.prev = r.fn(r.prev, m)
	rep.Result(r.prev)
}

func (r *reduceFrom[_]) Reset() {
	r.prev = r.init
}
