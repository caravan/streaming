package node_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"

	_table "github.com/caravan/streaming/internal/table"
)

type row struct {
	id    string
	name  string
	value string
}

func makeTestTable() (
	table.Table[string, string],
	table.Updater[*row, string, string],
) {
	tbl, _ := _table.Make[string, string]("id", "name", "value")

	updater, _ := _table.MakeUpdater(tbl,
		func(r *row) (string, error) {
			return r.id, nil
		},
		column.Make("id", func(r *row) (string, error) {
			return r.id, nil
		}),
		column.Make("name", func(r *row) (string, error) {
			return r.name, nil
		}),
		column.Make("value", func(r *row) (string, error) {
			return r.value, nil
		}),
	)

	return tbl, updater
}

func TestTableUpdater(t *testing.T) {
	as := assert.New(t)

	tbl, u := makeTestTable()

	updater := node.TableUpdater(u)
	lookup, _ := node.TableLookup(tbl, "value",
		func(r *row) (string, error) {
			return r.id, nil
		},
	)

	tester := node.Bind(updater, lookup)
	done := make(chan context.Done)
	in := make(chan *row)
	out := make(chan string)

	tester.Start(context.Make(done, make(chan error), in, out))
	in <- &row{
		id:    "some id",
		name:  "some name",
		value: "some value",
	}

	as.Equal("some value", <-out)
	close(done)
}

func TestTableLookup(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()
	err := updater.Update(&row{
		id:    "some id",
		name:  "some name",
		value: "some value",
	})
	as.Nil(err)

	lookup, err := node.TableLookup(tbl, "value",
		func(k string) (string, error) {
			return k, nil
		},
	)
	as.NotNil(lookup)
	as.Nil(err)

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan string)

	lookup.Start(context.Make(done, make(chan error), in, out))
	in <- "some id"
	as.Equal("some value", <-out)
	close(done)
}

func TestLookupCreateError(t *testing.T) {
	as := assert.New(t)

	tbl, _ := _table.Make[string, any]("not-missing")
	lookup, err := node.TableLookup(tbl, "missing",
		func(_ any) (string, error) {
			return "", nil
		},
	)
	as.Nil(lookup)
	as.EqualError(err, fmt.Sprintf(_table.ErrColumnNotFound, "missing"))
}

func TestLookupProcessError(t *testing.T) {
	as := assert.New(t)

	theKey := "the key"
	tbl, _ := _table.Make[string, any]("*")

	lookup, e := node.TableLookup(tbl, "*",
		func(e any) (string, error) {
			if err, ok := e.(error); ok {
				return "", err
			}
			return theKey, nil
		},
	)

	as.NotNil(lookup)
	as.Nil(e)

	done := make(chan context.Done)
	in := make(chan any)
	err := make(chan error)

	lookup.Start(context.Make(done, err, in, make(chan any)))

	in <- errors.New("key error")
	as.EqualError(<-err, "key error")

	in <- "missing"
	as.EqualError(<-err, fmt.Sprintf(_table.ErrKeyNotFound, theKey))
	close(done)
}
