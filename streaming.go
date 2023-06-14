package streaming

import (
	"github.com/caravan/streaming/stream"

	_stream "github.com/caravan/streaming/internal/stream"
)

// NewStream instantiates a new Stream, given a set of Processors
func NewStream[Msg any](p ...stream.Processor[Msg]) stream.Stream {
	return _stream.Make(p...)
}
