package table

import _table "github.com/caravan/streaming/table"

// The internal implementation of a TableUpdater
type updater[Msg any, Key comparable, Value any] struct {
	key     _table.KeySelector[Msg, Key]
	columns []_table.ColumnSelector[Msg, Value]
	table   _table.Table[Key, Value]
	setter  _table.Setter[Key, Value]
}

// MakeUpdater instantiates a new internal Updater instance
func MakeUpdater[Msg any, Key comparable, Value any](
	tbl _table.Table[Key, Value],
	key _table.KeySelector[Msg, Key],
	cols ..._table.ColumnSelector[Msg, Value],
) (_table.Updater[Msg, Key, Value], error) {
	names := make([]_table.ColumnName, len(cols))
	for i, c := range cols {
		names[i] = c.Name()
	}
	setter, err := tbl.Setter(names...)
	if err != nil {
		return nil, err
	}
	return &updater[Msg, Key, Value]{
		key:     key,
		columns: cols,
		table:   tbl,
		setter:  setter,
	}, nil
}

func (t *updater[Msg, Key, _]) Key() _table.KeySelector[Msg, Key] {
	return t.key
}

func (t *updater[Msg, _, Value]) Columns() []_table.ColumnSelector[Msg, Value] {
	return t.columns
}

// Update adds or overwrites a message in the Table. The message is associated
// with a Key that is selected from the message using the Table's KeySelector
func (t *updater[Msg, _, Value]) Update(msg Msg) error {
	k, err := t.key(msg)
	if err != nil {
		return err
	}
	row := make([]Value, len(t.columns))
	for i, s := range t.columns {
		v, err := s.Select(msg)
		if err != nil {
			return err
		}
		row[i] = v
	}
	return t.setter(k, row...)
}
