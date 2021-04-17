package column_test

import (
	"testing"

	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"
)

func TestColumn(t *testing.T) {
	as := assert.New(t)

	c := column.Make("some-col", func(e topic.Event) (table.Value, error) {
		return "hello", nil
	})

	as.Equal(table.ColumnName("some-col"), c.Name())

	sel := c.Selector()
	as.NotNil(sel)
	res, err := sel("anything")
	as.Equal("hello", res)
	as.Nil(err)
}
