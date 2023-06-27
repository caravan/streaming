package column

import "github.com/caravan/streaming/table"

type column[Msg, Value any] struct {
	name     table.ColumnName
	selector table.ValueSelector[Msg, Value]
}

// Make instantiates a new ColumnSelector instance
func Make[Msg, Value any](
	n table.ColumnName, s table.ValueSelector[Msg, Value],
) table.ColumnSelector[Msg, Value] {
	return &column[Msg, Value]{
		name:     n,
		selector: s,
	}
}

func (c *column[_, _]) Name() table.ColumnName {
	return c.name
}

func (c *column[Msg, Value]) Select(msg Msg) (Value, error) {
	return c.selector(msg)
}
