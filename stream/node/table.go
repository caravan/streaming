package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/table"
)

type (
	tableLookup[Msg, Res any] struct {
		table.KeySelector[Msg]
		table.ColumnSelector[Res]
	}

	tableSink[Msg, Res any] struct {
		table.Table[Msg, Res]
	}
)

// TableLookup performs a lookup on a table using the provided Event. The
// KeySelector extracts a Key from this Event and uses it to perform the lookup
// against the Table. The Column returned by the lookup is forwarded to the
// next Processor
func TableLookup[Msg, Value any](
	t table.Table[Msg, Value], c table.ColumnName, k table.KeySelector[Msg],
) (stream.Processor[Msg, Value], error) {
	getKey := t.KeySelector()
	getColumn, err := t.Selector(c)
	if err != nil {
		return nil, err
	}
	return func(msg Msg, rep stream.Reporter[Value]) {
		k, err := getKey(msg)
		if err != nil {
			var zero Value
			rep(zero, err)
			return
		}
		res, err := getColumn(k)
		if err != nil {
			var zero Value
			rep(zero, err)
			return
		}
		rep(res[0], nil)
	}, nil
}

// TableSink constructs a processor that sends all Events it sees to the
// provided Table
func TableSink[Msg, Res any](
	t table.Table[Msg, Res],
) stream.Processor[Msg, Msg] {
	return func(msg Msg, rep stream.Reporter[Msg]) {
		if _, err := t.Update(msg); err != nil {
			var zero Msg
			rep(zero, err)
			return
		}
		rep(msg, nil)
	}
}
