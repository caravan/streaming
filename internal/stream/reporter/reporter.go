package reporter

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
)

type (
	// Result is how a processor node reports a Result for further Stream
	// processing. A processor need not produce a Result or an Error
	Result func(topic.Event)

	// Error is how a processor node reports an Error to the active Stream
	// subprocess
	Error func(error)

	// Reporter is the internal representation of a stream.Reporter
	Reporter struct {
		result func(topic.Event)
		error  func(error)
	}
)

// Make constructs a new internal Reporter instance
func Make(res Result, err Error) *Reporter {
	return &Reporter{
		result: res,
		error:  err,
	}
}

// Wrap wraps a stream.Reporter as an internal implementation
func Wrap(r stream.Reporter) *Reporter {
	if c, ok := r.(*Reporter); ok {
		return c
	}
	return &Reporter{
		result: r.Result,
		error:  r.Error,
	}
}

// Result provided for further Stream processing
func (r *Reporter) Result(e topic.Event) {
	r.result(e)
}

// Error provided for Stream problem reporting
func (r *Reporter) Error(err error) {
	r.error(err)
}

// WithResult returns a copy of the Reporter with a new Result reporter
func (r *Reporter) WithResult(result Result) *Reporter {
	res := *r
	res.result = result
	return &res
}

// WithError returns a copy of the Reporter with a new Error reporter
func (r *Reporter) WithError(error Error) *Reporter {
	res := *r
	res.error = error
	return &res
}
