package table_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/streaming/internal/table"
)

type tableRow struct {
	key  string
	name string
	age  int
}

func TestUpdater(t *testing.T) {
	as := assert.New(t)

	tbl, err := internal.Make[string, any]("name", "age")
	as.NotNil(tbl)
	as.Nil(err)

	getter, err := tbl.Getter("name", "age")
	as.NotNil(getter)
	as.Nil(err)

	updater, err := internal.MakeUpdater(tbl,
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
	as.Equal([]any{"bill", 42}, res)

	err = updater.Update(&tableRow{
		key:  secondID,
		name: "carol",
		age:  47,
	})
	as.Nil(err)
	res, _ = getter(secondID)
	as.Equal([]any{"carol", 47}, res)

	sel, err := tbl.Getter("age", "name")
	as.NotNil(sel)
	as.Nil(err)

	res, err = sel(secondID)
	as.Nil(err)
	as.Equal([]any{47, "carol"}, res)

	res, err = sel(firstID)
	as.Nil(err)
	as.Equal([]any{42, "bill"}, res)

	missing := "missing"
	res, err = sel(missing)
	as.Nil(res)
	as.EqualError(err, fmt.Sprintf(table.ErrKeyNotFound, missing))
}

func TestBadUpdater(t *testing.T) {
	as := assert.New(t)

	emptyTable, err := internal.Make[string, any]()
	as.NotNil(emptyTable)
	as.Nil(err)

	updater, err := internal.MakeUpdater[*tableRow, string, any](emptyTable,
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
	as.Nil(updater)
	as.Errorf(err, table.ErrColumnNotFound, "explode")
}

func TestBadSelectors(t *testing.T) {
	as := assert.New(t)

	tbl, err := internal.Make[string, any]("explode")
	as.NotNil(tbl)
	as.Nil(err)

	updater, _ := internal.MakeUpdater[*tableRow, string, any](tbl,
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
