package column_test

import (
	"testing"

	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"
)

func TestColumn(t *testing.T) {
	as := assert.New(t)

	c := column.Make[any, string]("some-col", func(_ any) (string, error) {
		return "hello", nil
	})

	as.Equal(table.ColumnName("some-col"), c.Name())
	res, err := c.Select("anything")
	as.Equal("hello", res)
	as.Nil(err)
}
