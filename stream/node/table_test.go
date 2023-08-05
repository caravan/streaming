package node_test

import (
	"fmt"
	"testing"

	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"
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
	tbl, _ := streaming.NewTable[string, string]("id", "name", "value")

	updater, _ := streaming.NewTableUpdater(tbl,
		func(r *row) string {
			return r.id
		},
		column.Make("id", func(r *row) string {
			return r.id
		}),
		column.Make("name", func(r *row) string {
			return r.name
		}),
		column.Make("value", func(r *row) string {
			return r.value
		}),
	)

	return tbl, updater
}

func TestTableUpdater(t *testing.T) {
	as := assert.New(t)

	tbl, u := makeTestTable()

	updater := node.TableUpdater(u)
	lookup, _ := node.TableLookup(tbl, "value",
		func(r *row) string {
			return r.id
		},
	)

	tester := node.Bind(updater, lookup)
	done := make(chan context.Done)
	in := make(chan *row)
	out := make(chan string)

	tester.Start(context.Make(done, make(chan context.Advice), in, out))
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
		func(k string) string {
			return k
		},
	)
	as.NotNil(lookup)
	as.Nil(err)

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan string)

	lookup.Start(context.Make(done, make(chan context.Advice), in, out))
	in <- "some id"
	as.Equal("some value", <-out)
	close(done)
}

func TestLookupCreateError(t *testing.T) {
	as := assert.New(t)

	tbl, _ := streaming.NewTable[string, any]("not-missing")
	lookup, err := node.TableLookup(tbl, "missing",
		func(_ any) string {
			return ""
		},
	)
	as.Nil(lookup)
	as.EqualError(err, fmt.Sprintf(table.ErrColumnNotFound, "missing"))
}

func TestLookupProcessError(t *testing.T) {
	as := assert.New(t)

	theKey := "the key"
	tbl, _ := streaming.NewTable[string, any]("*")

	lookup, e := node.TableLookup(tbl, "*",
		func(e any) string {
			return theKey
		},
	)

	as.NotNil(lookup)
	as.Nil(e)

	done := make(chan context.Done)
	in := make(chan any)
	monitor := make(chan context.Advice)

	lookup.Start(context.Make(done, monitor, in, make(chan any)))

	in <- "missing"
	as.EqualError(
		(<-monitor).(error), fmt.Sprintf(table.ErrKeyNotFound, theKey),
	)
	close(done)
}
