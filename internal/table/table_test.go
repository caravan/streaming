package table_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"

	_table "github.com/caravan/streaming/internal/table"
)

type tableRow struct {
	key  id.ID
	name string
	age  int
}

func TestTable(t *testing.T) {
	as := assert.New(t)

	tbl := streaming.NewTable(
		func(e topic.Event) (table.Key, error) {
			return e.(*tableRow).key, nil
		},
		column.Make("name", func(e topic.Event) (table.Value, error) {
			return e.(*tableRow).name, nil
		}),
		column.Make("age", func(e topic.Event) (table.Value, error) {
			return e.(*tableRow).age, nil
		}),
	)

	as.NotNil(tbl)

	firstID := id.New()
	secondID := id.New()

	res, err := tbl.Update(&tableRow{
		key:  firstID,
		name: "bill",
		age:  42,
	})
	as.Nil(err)
	as.Equal(table.Relation{"bill", 42}, res)

	res, err = tbl.Update(&tableRow{
		key:  secondID,
		name: "carol",
		age:  47,
	})
	as.Nil(err)
	as.Equal(table.Relation{"carol", 47}, res)

	sel, err := tbl.Selector("age", "name")
	as.NotNil(sel)
	as.Nil(err)

	res, err = sel(secondID)
	as.Nil(err)
	as.Equal(table.Relation{47, "carol"}, res)

	res, err = sel(firstID)
	as.Nil(err)
	as.Equal(table.Relation{42, "bill"}, res)

	missing := id.New()
	res, err = sel(missing)
	as.Nil(res)
	as.EqualError(err, fmt.Sprintf(_table.ErrKeyNotFound, missing))
}

func TestMissingColumn(t *testing.T) {
	as := assert.New(t)

	tbl := streaming.NewTable(
		func(e topic.Event) (table.Key, error) {
			return e.(*tableRow).key, nil
		},
	)

	sel, err := tbl.Selector("not-found")
	as.Nil(sel)
	as.EqualError(err, fmt.Sprintf(_table.ErrColumnNotFound, "not-found"))
}

func TestBadSelectors(t *testing.T) {
	as := assert.New(t)

	tbl := streaming.NewTable(
		func(e topic.Event) (table.Key, error) {
			if e == nil || e.(*tableRow).key == id.Nil {
				return id.Nil, errors.New("key-error")
			}
			return e.(*tableRow).key, nil
		},
		column.Make("explode", func(e topic.Event) (table.Value, error) {
			return id.Nil, errors.New("column-error")
		}),
	)

	_, err := tbl.Update(nil)
	as.EqualError(err, "key-error")

	keySel := tbl.KeySelector()
	res1, err := keySel(nil)
	as.Equal(id.Nil, res1)
	as.EqualError(err, "key-error")

	cols1, err := tbl.Selector("explode")
	as.Nil(err)
	as.NotNil(cols1)

	found := id.New()
	_, err = tbl.Update(&tableRow{
		key: found,
	})
	as.EqualError(err, "column-error")

	cols2 := tbl.Columns()
	as.NotNil(cols2)
	res3, err := cols2[0].Selector()(nil)
	as.Equal(id.Nil, res3)
	as.EqualError(err, "column-error")
}
