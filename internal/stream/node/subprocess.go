package node

import (
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/stream"
)

type (
	subprocess[Msg any] []stream.Processor[Msg]

	statefulSubprocess[Msg any] struct {
		stream.Processor[Msg]
		stateful []stream.StatefulProcessor[Msg]
	}
)

// Subprocess is the internal implementation of a Subprocess
func Subprocess[Msg any](p ...stream.Processor[Msg]) stream.Processor[Msg] {
	res := make(subprocess[Msg], 0, len(p))
	var stateful []stream.StatefulProcessor[Msg]
	for _, e := range p {
		if _, ok := e.(Forward[Msg]); ok {
			continue
		}
		res = append(res, e)
		if st, ok := e.(stream.StatefulProcessor[Msg]); ok {
			stateful = append(stateful, st)
		}
	}

	switch len(res) {
	case 0:
		return Forward[Msg]{}
	case 1:
		return res[0]
	default:
		var result stream.Processor[Msg] = res
		if len(stateful) > 0 {
			result = &statefulSubprocess[Msg]{
				Processor: result,
				stateful:  stateful,
			}
		}
		return result
	}
}

func (s subprocess[Msg]) Process(m Msg, r stream.Reporter[Msg]) {
	wr := reporter.Wrap(r)

	var nextContinue func(idx int) stream.Reporter[Msg]
	nextContinue = func(idx int) stream.Reporter[Msg] {
		if idx >= len(s) {
			return r
		}
		return wr.WithResult(func(m Msg) {
			s[idx].Process(m, nextContinue(idx+1))
		})
	}

	nextContinue(0).Result(m)
}

func (s *statefulSubprocess[_]) Reset() {
	for _, p := range s.stateful {
		p.Reset()
	}
}
