package node_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/caravan/essentials/id"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"

	_table "github.com/caravan/streaming/internal/table"
)

func TestTableLookup(t *testing.T) {
	as := assert.New(t)

	theID := id.New()
	tbl := _table.Make[string, string](
		func(_ string) (table.Key, error) {
			return theID, nil
		},
		column.Make("*", func(s string) (string, error) {
			return s, nil
		}),
	)
	res, err := tbl.Update("some value")
	as.Nil(err)
	as.Equal(table.Relation[string]{"some value"}, res)

	lookup, err := node.TableLookup(tbl, "*",
		func(_ string) (table.Key, error) {
			return theID, nil
		},
	)
	as.NotNil(lookup)
	as.Nil(err)

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan string)

	lookup.Start(context.Make(done, make(chan error), in, out))
	in <- "anything"
	as.Equal("some value", <-out)
	close(done)
}

func TestLookupCreateError(t *testing.T) {
	as := assert.New(t)

	tbl := _table.Make[any, any](nil)
	lookup, err := node.TableLookup(tbl, "missing",
		func(_ any) (table.Key, error) {
			return id.Nil, nil
		},
	)
	as.Nil(lookup)
	as.EqualError(err, fmt.Sprintf(_table.ErrColumnNotFound, "missing"))
}

func TestLookupProcessError(t *testing.T) {
	as := assert.New(t)

	theKey := id.New()
	tbl := _table.Make[any, any](
		func(_ any) (table.Key, error) {
			return theKey, nil
		},
		column.Make("*", func(e any) (any, error) {
			return e, nil
		}),
	)

	lookup, e := node.TableLookup(tbl, "*",
		func(e any) (table.Key, error) {
			if err, ok := e.(error); ok {
				return id.Nil, err
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
