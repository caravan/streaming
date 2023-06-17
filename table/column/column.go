package column

import "github.com/caravan/streaming/table"

type column[Msg, Value any] struct {
	name     table.ColumnName
	selector table.Selector[Msg, Value]
}

// Make instantiates a new Column instance
func Make[Msg, Value any](
	n table.ColumnName, s table.Selector[Msg, Value],
) table.Column[Msg, Value] {
	return &column[Msg, Value]{
		name:     n,
		selector: s,
	}
}

func (c *column[_, _]) Name() table.ColumnName {
	return c.name
}

func (c *column[Msg, Value]) Selector() table.Selector[Msg, Value] {
	return c.selector
}
