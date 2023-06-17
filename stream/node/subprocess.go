package node

import "github.com/caravan/streaming/stream"

// Subprocess is the internal implementation of a Subprocess
func Subprocess[Msg any](
	p ...stream.Processor[Msg, Msg],
) stream.Processor[Msg, Msg] {
	switch len(p) {
	case 0:
		return Forward[Msg]
	case 1:
		return p[0]
	case 2:
		return stream.Combine[Msg, Msg](p[0], p[1])
	default:
		return stream.Combine[Msg, Msg](p[0], Subprocess(p[1:]...))
	}
}
