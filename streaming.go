package streaming

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"

	_stream "github.com/caravan/streaming/internal/stream"
	_table "github.com/caravan/streaming/internal/table"
)

// NewStream instantiates a new stream, given a set of Processors
func NewStream[Msg any](p ...stream.Processor[Msg, Msg]) stream.Stream {
	return _stream.Make(node.Subprocess(p...))
}

// NewTable instantiates a new Table given a set of column names
func NewTable[Key comparable, Value any](
	c ...table.ColumnName,
) (table.Table[Key, Value], error) {
	return _table.Make[Key, Value](c...)
}

// NewTableUpdater instantiates a new table Updater given a Table and a set of
// Key and Column Selectors
func NewTableUpdater[Msg any, Key comparable, Value any](
	t table.Table[Key, Value],
	k table.KeySelector[Msg, Key],
	c ...table.ColumnSelector[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	return _table.MakeUpdater[Msg, Key, Value](t, k, c...)
}
