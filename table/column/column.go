package column

import "github.com/caravan/streaming/table"

type column struct {
	name     table.ColumnName
	selector table.Selector
}

// Make instantiates a new Column instance
func Make(n table.ColumnName, s table.Selector) table.Column {
	return &column{
		name:     n,
		selector: s,
	}
}

func (c *column) Name() table.ColumnName {
	return c.name
}

func (c *column) Selector() table.Selector {
	return c.selector
}
