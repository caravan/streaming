package node_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/caravan/essentials/id"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"

	_table "github.com/caravan/streaming/internal/table"
)

func TestTableLookup(t *testing.T) {
	as := assert.New(t)

	theID := id.New()
	tbl := streaming.NewTable(
		func(_ stream.Event) (table.Key, error) {
			return theID, nil
		},
		column.Make("*", func(e stream.Event) (table.Value, error) {
			return e, nil
		}),
	)
	res, err := tbl.Update("some value")
	as.Nil(err)
	as.Equal(table.Relation{"some value"}, res)

	lookup, err := node.TableLookup(tbl, "*",
		func(_ stream.Event) (table.Key, error) {
			return theID, nil
		},
	)
	as.NotNil(lookup)
	as.Nil(err)

	lookup.Process("anything", reporter.Make(
		func(e stream.Event) {
			as.Equal("some value", e)
		},
		func(e error) {
			as.Fail("no error here")
		},
	))
}

func TestLookupCreateError(t *testing.T) {
	as := assert.New(t)

	tbl := streaming.NewTable(nil)
	lookup, err := node.TableLookup(tbl, "missing",
		func(_ stream.Event) (table.Key, error) {
			return id.Nil, nil
		},
	)
	as.Nil(lookup)
	as.EqualError(err, fmt.Sprintf(_table.ErrColumnNotFound, "missing"))
}

func TestLookupProcessError(t *testing.T) {
	as := assert.New(t)

	theKey := id.New()
	tbl := streaming.NewTable(
		func(_ stream.Event) (table.Key, error) {
			return theKey, nil
		},
		column.Make("*", func(e stream.Event) (table.Value, error) {
			return e, nil
		}),
	)

	lookup, err := node.TableLookup(tbl, "*",
		func(e stream.Event) (table.Key, error) {
			if err, ok := e.(error); ok {
				return id.Nil, err
			}
			return theKey, nil
		},
	)

	as.NotNil(lookup)
	as.Nil(err)

	lookup.Process(errors.New("key error"), reporter.Make(
		func(_ stream.Event) {
			as.Fail("no result here")
		},
		func(err error) {
			as.EqualError(err, "key error")
		},
	))

	lookup.Process("missing", reporter.Make(
		func(_ stream.Event) {
			as.Fail("no result here")
		},
		func(err error) {
			as.EqualError(err, fmt.Sprintf(_table.ErrKeyNotFound, theKey))
		},
	))
}

func TestTableSink(t *testing.T) {
	node.TableSink(nil).Sink()
}

func TestTableSinkError(t *testing.T) {
	as := assert.New(t)

	tbl := streaming.NewTable(func(e stream.Event) (table.Key, error) {
		return id.Nil, errors.New("key error")
	})

	s := node.TableSink(tbl)
	s.Process("some value", reporter.Make(
		func(e stream.Event) {
			as.Fail("no result here")
		},
		func(e error) {
			as.EqualError(e, "key error")
		},
	))
}
