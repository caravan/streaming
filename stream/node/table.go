package node

import (
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/table"
)

// TableLookup performs a lookup on a table using the provided message. The
// KeySelector extracts a Key from this message and uses it to perform the
// lookup against the Table. The Column returned by the lookup is forwarded to
// the next Processor
func TableLookup[Msg, Value any](
	t table.Table[Msg, Value], c table.ColumnName, k table.KeySelector[Msg],
) (stream.Processor[Msg, Value], error) {
	getColumn, err := t.Selector(c)
	if err != nil {
		return nil, err
	}
	return func(c *context.Context[Msg, Value]) {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if k, e := k(msg); e != nil {
				if !c.ReportError(e) {
					return
				}
			} else if res, e := getColumn(k); e != nil {
				if !c.ReportError(e) {
					return
				}
			} else if !c.ForwardResult(res[0]) {
				return
			}
		}
	}, nil
}

// TableUpdater constructs a processor that sends all messages it sees to the
// provided Table
func TableUpdater[Msg, Res any](
	t table.Table[Msg, Res],
) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if _, e := t.Update(msg); e != nil {
				if !c.ReportError(e) {
					return
				}
			} else if !c.ForwardResult(msg) {
				return
			}
		}
	}
}
