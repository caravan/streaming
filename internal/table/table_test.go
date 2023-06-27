package table_test

import (
	"fmt"
	"testing"

	"github.com/caravan/streaming/table"
	"github.com/stretchr/testify/assert"

	_table "github.com/caravan/streaming/internal/table"
)

type tableRow struct {
	key  string
	name string
	age  int
}

func TestTable(t *testing.T) {
	as := assert.New(t)

	tbl, err := _table.Make[string, any]("name", "age")
	as.NotNil(tbl)
	as.Nil(err)

	getter, err := tbl.Getter("name", "age")
	as.NotNil(getter)
	as.Nil(err)

	setter, err := tbl.Setter("name", "age")
	as.NotNil(setter)
	as.Nil(err)

	firstID := "first id"
	secondID := "second id"

	err = setter(firstID, "bill", 42)
	as.Nil(err)

	err = setter(secondID, "carol", 47)
	as.Nil(err)

	res, _ := getter(firstID)
	as.Equal(table.Relation[any]{"bill", 42}, res)

	res, _ = getter(secondID)
	as.Equal(table.Relation[any]{"carol", 47}, res)

	missing := "missing"
	res, err = getter(missing)
	as.Nil(res)
	as.EqualError(err, fmt.Sprintf(_table.ErrKeyNotFound, missing))
}

func TestBadTable(t *testing.T) {
	as := assert.New(t)
	tbl, err := _table.Make[string, any]("column-1", "column-2", "column-1")
	as.Nil(tbl)
	as.Errorf(err, _table.ErrDuplicateColumnName, "column-1")
}

func TestMissingColumn(t *testing.T) {
	as := assert.New(t)

	tbl, err := _table.Make[string, any]("column-1", "column-2")
	as.NotNil(tbl)
	as.Nil(err)

	as.Equal([]table.ColumnName{"column-1", "column-2"}, tbl.Columns())
	sel, err := tbl.Getter("not-found")
	as.Nil(sel)
	as.EqualError(err, fmt.Sprintf(_table.ErrColumnNotFound, "not-found"))
}

func TestBadSetter(t *testing.T) {
	as := assert.New(t)

	tbl, err := _table.Make[string, any]("column-1", "column-2")
	as.NotNil(tbl)
	as.Nil(err)

	s, err := tbl.Setter("column-1", "column-2", "column-1")
	as.Nil(s)
	as.Errorf(err, _table.ErrDuplicateColumnName, "column-1")

	s, err = tbl.Setter("column-1", "column-2", "column-3")
	as.Nil(s)
	as.EqualError(err, fmt.Sprintf(_table.ErrColumnNotFound, "column-3"))

	s, err = tbl.Setter("column-1", "column-2")
	as.NotNil(s)
	as.Nil(err)

	err = s("some-key", "too few")
	as.Errorf(err, fmt.Sprintf(_table.ErrValueCountRequired, 2, 1))

	err = s("some-key", "one", "too", "many")
	as.Errorf(err, fmt.Sprintf(_table.ErrValueCountRequired, 2, 3))
}
