package reporter

import "github.com/caravan/streaming/stream"

type (
	// Result is how a processor node reports a Result for further Stream
	// processing. A processor need not produce a Result or an Error
	Result[Msg any] func(Msg)

	// Error is how a processor node reports an Error to the active Stream
	// subprocess
	Error func(error)

	// Reporter is the internal representation of a stream.Reporter
	Reporter[Msg any] struct {
		result func(Msg)
		error  func(error)
	}
)

// Make constructs a new internal Reporter instance
func Make[Msg any](res Result[Msg], err Error) *Reporter[Msg] {
	return &Reporter[Msg]{
		result: res,
		error:  err,
	}
}

// Wrap wraps a stream.Reporter as an internal implementation
func Wrap[Msg any](r stream.Reporter[Msg]) *Reporter[Msg] {
	if c, ok := r.(*Reporter[Msg]); ok {
		return c
	}
	return &Reporter[Msg]{
		result: r.Result,
		error:  r.Error,
	}
}

// Result provided for further Stream processing
func (r *Reporter[Msg]) Result(m Msg) {
	r.result(m)
}

// Error provided for Stream problem reporting
func (r *Reporter[Msg]) Error(err error) {
	r.error(err)
}

// WithResult returns a copy of the Reporter with a new Result reporter
func (r *Reporter[Msg]) WithResult(result Result[Msg]) *Reporter[Msg] {
	res := *r
	res.result = result
	return &res
}

// WithError returns a copy of the Reporter with a new Error reporter
func (r *Reporter[Msg]) WithError(error Error) *Reporter[Msg] {
	res := *r
	res.error = error
	return &res
}
