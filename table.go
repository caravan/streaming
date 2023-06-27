package streaming

import (
	"github.com/caravan/streaming/table"

	internal "github.com/caravan/streaming/internal/table"
)

// NewTable instantiates a new Table given a set of column names
func NewTable[Key comparable, Value any](
	c ...table.ColumnName,
) (table.Table[Key, Value], error) {
	return internal.Make[Key, Value](c...)
}

// NewTableUpdater instantiates a new table Updater given a Table and a set of
// Key and Column Selectors
func NewTableUpdater[Msg any, Key comparable, Value any](
	t table.Table[Key, Value],
	k table.KeySelector[Msg, Key],
	c ...table.ColumnSelector[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	return internal.MakeUpdater[Msg, Key, Value](t, k, c...)
}
