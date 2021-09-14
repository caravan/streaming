package node

import (
	"github.com/caravan/essentials/event"
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/stream"
)

type (
	subprocess []stream.Processor

	statefulSubprocess struct {
		stream.Processor
		stateful []stream.StatefulProcessor
	}
)

// Subprocess is the internal implementation of a Subprocess
func Subprocess(p ...stream.Processor) stream.Processor {
	res := make(subprocess, 0, len(p))
	var stateful []stream.StatefulProcessor
	for _, e := range p {
		if _, ok := e.(forward); ok {
			continue
		}
		res = append(res, e)
		if st, ok := e.(stream.StatefulProcessor); ok {
			stateful = append(stateful, st)
		}
	}

	switch len(res) {
	case 0:
		return Forward
	case 1:
		return res[0]
	default:
		var result stream.Processor = res
		if len(stateful) > 0 {
			result = &statefulSubprocess{
				Processor: result,
				stateful:  stateful,
			}
		}
		return result
	}
}

func (s subprocess) Process(e event.Event, r stream.Reporter) {
	wr := reporter.Wrap(r)

	var nextContinue func(idx int) stream.Reporter
	nextContinue = func(idx int) stream.Reporter {
		if idx >= len(s) {
			return r
		}
		return wr.WithResult(func(e event.Event) {
			s[idx].Process(e, nextContinue(idx+1))
		})
	}

	nextContinue(0).Result(e)
}

func (s *statefulSubprocess) Reset() {
	for _, p := range s.stateful {
		p.Reset()
	}
}
