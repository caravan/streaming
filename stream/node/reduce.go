package node

import (
	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming/stream"
)

type (
	// Reducer is the signature for a function that can perform Stream
	// reduction. The Event that is returned will be passed downstream
	Reducer func(message.Event, message.Event) message.Event

	reduce struct {
		fn   Reducer
		prev message.Event
		rest bool
	}

	reduceFrom struct {
		fn   Reducer
		init message.Event
		prev message.Event
	}
)

// Reduce constructs a processor that reduces the Events it sees into some
// form of aggregated Events, based on the provided function
func Reduce(fn Reducer) stream.Processor {
	return &reduce{
		fn: fn,
	}
}

// ReduceFrom constructs a processor that reduces the Events it sees into
// some form of aggregated Events, based on the provided function and
// an initial Event
func ReduceFrom(fn Reducer, init message.Event) stream.Processor {
	return &reduceFrom{
		fn:   fn,
		init: init,
		prev: init,
	}
}

func (r *reduce) Process(e message.Event, rep stream.Reporter) {
	if !r.rest {
		r.rest = true
		r.prev = e
		return
	}
	r.prev = r.fn(r.prev, e)
	rep.Result(r.prev)
}

func (r *reduce) Reset() {
	r.prev = nil
	r.rest = false
}

func (r *reduceFrom) Process(e message.Event, rep stream.Reporter) {
	r.prev = r.fn(r.prev, e)
	rep.Result(r.prev)
}

func (r *reduceFrom) Reset() {
	r.prev = r.init
}
