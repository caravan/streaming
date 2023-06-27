package table_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"

	_table "github.com/caravan/streaming/internal/table"
)

func TestUpdater(t *testing.T) {
	as := assert.New(t)

	tbl, err := _table.Make[string, any]("name", "age")
	as.NotNil(tbl)
	as.Nil(err)

	getter, err := tbl.Getter("name", "age")
	as.NotNil(getter)
	as.Nil(err)

	updater, err := _table.MakeUpdater(tbl,
		func(e *tableRow) (string, error) {
			return e.key, nil
		},
		column.Make("name", func(r *tableRow) (any, error) {
			return r.name, nil
		}),
		column.Make("age", func(r *tableRow) (any, error) {
			return r.age, nil
		}),
	)
	as.NotNil(updater)
	as.Nil(err)

	firstID := "first id"
	secondID := "second id"

	err = updater.Update(&tableRow{
		key:  firstID,
		name: "bill",
		age:  42,
	})
	as.Nil(err)
	res, _ := getter(firstID)
	as.Equal(table.Relation[any]{"bill", 42}, res)

	err = updater.Update(&tableRow{
		key:  secondID,
		name: "carol",
		age:  47,
	})
	as.Nil(err)
	res, _ = getter(secondID)
	as.Equal(table.Relation[any]{"carol", 47}, res)

	sel, err := tbl.Getter("age", "name")
	as.NotNil(sel)
	as.Nil(err)

	res, err = sel(secondID)
	as.Nil(err)
	as.Equal(table.Relation[any]{47, "carol"}, res)

	res, err = sel(firstID)
	as.Nil(err)
	as.Equal(table.Relation[any]{42, "bill"}, res)

	missing := "missing"
	res, err = sel(missing)
	as.Nil(res)
	as.EqualError(err, fmt.Sprintf(_table.ErrKeyNotFound, missing))
}

func TestBadSelectors(t *testing.T) {
	as := assert.New(t)

	tbl, err := _table.Make[string, any]("explode")
	as.NotNil(tbl)
	as.Nil(err)

	updater, _ := _table.MakeUpdater[*tableRow, string, any](tbl,
		func(r *tableRow) (string, error) {
			if r == nil || r.key == "" {
				return "", errors.New("key-error")
			}
			return r.key, nil
		},
		column.Make("explode", func(e *tableRow) (any, error) {
			return "", errors.New("column-error")
		}),
	)

	err = updater.Update(nil)
	as.EqualError(err, "key-error")

	keySel := updater.Key()
	res1, err := keySel(nil)
	as.Equal("", res1)
	as.EqualError(err, "key-error")

	cols1, err := tbl.Getter("explode")
	as.Nil(err)
	as.NotNil(cols1)

	found := "found"
	err = updater.Update(&tableRow{
		key: found,
	})
	as.EqualError(err, "column-error")

	cols2 := updater.Columns()
	as.NotNil(cols2)
	res3, err := cols2[0].Select(nil)
	as.Equal("", res3)
	as.EqualError(err, "column-error")
}
