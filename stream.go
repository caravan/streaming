package streaming

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"

	internal "github.com/caravan/streaming/internal/stream"
)

// NewStream instantiates a new stream, given a set of Processors
func NewStream[Msg any](
	source stream.Processor[stream.Source, Msg],
	rest ...stream.Processor[Msg, Msg],
) stream.Stream {
	return internal.Make(source, node.Subprocess(rest...))
}
