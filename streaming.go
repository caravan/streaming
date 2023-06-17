package streaming

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"
)

// NewStream instantiates a new stream, given a set of Processors
func NewStream[Msg any](p ...stream.Processor[Msg, Msg]) stream.Stream {
	return stream.Make(node.Subprocess(p...))
}

// NewTable instantiates a new Table, given a key selector and a set of column
// definitions
func NewTable[Msg any](
	k table.KeySelector[Msg], c ...table.Column[Msg, Msg],
) table.Table[Msg, Msg] {
	return table.Make(k, c...)
}
