package node

import (
	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/table"
)

type (
	tableLookup struct {
		table.KeySelector
		table.ColumnSelector
	}

	tableSink struct {
		table.Table
	}
)

// TableLookup performs a lookup on a table using the provided Event. The
// KeySelector extracts a Key from this Event and uses it to perform the lookup
// against the Table. The Column returned by the lookup is forwarded to the
// next Processor
func TableLookup(
	t table.Table, c table.ColumnName, k table.KeySelector,
) (stream.Processor, error) {
	s, err := t.Selector(c)
	if err != nil {
		return nil, err
	}
	return &tableLookup{
		KeySelector:    k,
		ColumnSelector: s,
	}, nil
}

func (t tableLookup) Process(e message.Event, r stream.Reporter) {
	k, err := t.KeySelector(e)
	if err != nil {
		r.Error(err)
		return
	}
	res, err := t.ColumnSelector(k)
	if err != nil {
		r.Error(err)
		return
	}
	r.Result(res[0])
}

// TableSink constructs a processor that sends all Events it sees to the
// provided Table
func TableSink(t table.Table) stream.SinkProcessor {
	return &tableSink{
		Table: t,
	}
}

func (*tableSink) Sink() {}

func (t *tableSink) Process(e message.Event, r stream.Reporter) {
	if _, err := t.Update(e); err != nil {
		r.Error(err)
	}
}
