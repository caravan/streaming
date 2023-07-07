package table

import "github.com/caravan/streaming/table"

// Updater is the internal implementation of a table.Updater
type Updater[Msg any, Key comparable, Value any] struct {
	key     table.KeySelector[Msg, Key]
	columns []table.ColumnSelector[Msg, Value]
	table   table.Table[Key, Value]
	setter  table.Setter[Key, Value]
}

// MakeUpdater instantiates a new internal Updater instance
func MakeUpdater[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value],
	key table.KeySelector[Msg, Key],
	cols ...table.ColumnSelector[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	names := make([]table.ColumnName, len(cols))
	for i, c := range cols {
		names[i] = c.Name()
	}
	setter, err := tbl.Setter(names...)
	if err != nil {
		return nil, err
	}
	return &Updater[Msg, Key, Value]{
		key:     key,
		columns: cols,
		table:   tbl,
		setter:  setter,
	}, nil
}

func (u *Updater[Msg, Key, _]) Key() table.KeySelector[Msg, Key] {
	return u.key
}

func (u *Updater[Msg, _, Value]) Columns() []table.ColumnSelector[Msg, Value] {
	return u.columns
}

// Update adds or overwrites a message in the Table. The message is associated
// with a Key that is selected from the message using the Table's KeySelector
func (u *Updater[Msg, _, Value]) Update(msg Msg) error {
	k, err := u.key(msg)
	if err != nil {
		return err
	}
	row := make([]Value, len(u.columns))
	for i, s := range u.columns {
		v, err := s.Select(msg)
		if err != nil {
			return err
		}
		row[i] = v
	}
	return u.setter(k, row...)
}
