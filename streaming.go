package streaming

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/table"

	_stream "github.com/caravan/streaming/internal/stream"
	_table "github.com/caravan/streaming/internal/table"
)

// NewStream instantiates a new Stream, given a set of Processors
func NewStream(p ...stream.Processor) stream.Stream {
	return _stream.Make(p...)
}

// NewTable instantiates a new Table, given a key selector and a set of column
// definitions
func NewTable(k table.KeySelector, c ...table.Column) table.Table {
	return _table.Make(k, c...)
}
