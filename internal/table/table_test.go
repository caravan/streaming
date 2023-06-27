package table_test

import (
	"fmt"
	"testing"

	"github.com/caravan/streaming/table"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/table"
)

func TestTable(t *testing.T) {
	as := assert.New(t)

	tbl, err := internal.Make[string, any]("name", "age")
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
	as.Equal([]any{"bill", 42}, res)

	res, _ = getter(secondID)
	as.Equal([]any{"carol", 47}, res)

	missing := "missing"
	res, err = getter(missing)
	as.Nil(res)
	as.EqualError(err, fmt.Sprintf(table.ErrKeyNotFound, missing))
}

func TestBadTable(t *testing.T) {
	as := assert.New(t)
	tbl, err := internal.Make[string, any]("column-1", "column-2", "column-1")
	as.Nil(tbl)
	as.Errorf(err, table.ErrDuplicateColumnName, "column-1")
}

func TestMissingColumn(t *testing.T) {
	as := assert.New(t)

	tbl, err := internal.Make[string, any]("column-1", "column-2")
	as.NotNil(tbl)
	as.Nil(err)

	as.Equal([]table.ColumnName{"column-1", "column-2"}, tbl.Columns())
	sel, err := tbl.Getter("not-found")
	as.Nil(sel)
	as.EqualError(err, fmt.Sprintf(table.ErrColumnNotFound, "not-found"))
}

func TestCompetingSetters(t *testing.T) {
	as := assert.New(t)

	tbl, _ := internal.Make[string, any]("name", "age")
	allSetter, _ := tbl.Setter("name", "age")
	getter, _ := tbl.Getter("name", "age")

	nameSetter, err := tbl.Setter("name")
	as.NotNil(nameSetter)
	as.Nil(err)

	ageSetter, err := tbl.Setter("age")
	as.NotNil(ageSetter)
	as.Nil(err)

	as.Nil(allSetter("1", "bob", 42))
	as.Nil(allSetter("2", "june", 36))

	row, _ := getter("1")
	as.Equal([]any{"bob", 42}, row)
	row, _ = getter("2")
	as.Equal([]any{"june", 36}, row)

	as.Nil(nameSetter("1", "robert"))
	as.Nil(ageSetter("2", 41))

	row, _ = getter("1")
	as.Equal([]any{"robert", 42}, row)

	row, _ = getter("2")
	as.Equal([]any{"june", 41}, row)
}

func TestBadSetter(t *testing.T) {
	as := assert.New(t)

	tbl, err := internal.Make[string, any]("column-1", "column-2")
	as.NotNil(tbl)
	as.Nil(err)

	s, err := tbl.Setter("column-1", "column-2", "column-1")
	as.Nil(s)
	as.Errorf(err, table.ErrDuplicateColumnName, "column-1")

	s, err = tbl.Setter("column-1", "column-2", "column-3")
	as.Nil(s)
	as.EqualError(err, fmt.Sprintf(table.ErrColumnNotFound, "column-3"))

	s, err = tbl.Setter("column-1", "column-2")
	as.NotNil(s)
	as.Nil(err)

	err = s("some-key", "too few")
	as.Errorf(err, fmt.Sprintf(table.ErrValueCountRequired, 2, 1))

	err = s("some-key", "one", "too", "many")
	as.Errorf(err, fmt.Sprintf(table.ErrValueCountRequired, 2, 3))
}
