package build_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/receiver"
	"github.com/caravan/essentials/sender"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/build"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"
)

func TestPump(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic()
	out := essentials.NewTopic()

	s, err := build.TopicSource(in).TopicSink(out).Stream()
	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	sender.Send(p, 1)
	sender.Send(p, 2)
	sender.Send(p, 3)

	as.Equal(1, receiver.MustReceive(c))
	as.Equal(2, receiver.MustReceive(c))
	as.Equal(3, receiver.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestFilterMapReduce(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic()
	out := essentials.NewTopic()

	s, err := build.
		TopicSource(in).
		Filter(func(e event.Event) bool {
			return e.(int)%2 == 0
		}).
		Map(func(e event.Event) event.Event {
			return e.(int) * 3
		}).
		Reduce(func(l event.Event, r event.Event) event.Event {
			return l.(int) + r.(int)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	sender.Send(p, 1)
	sender.Send(p, 2)
	sender.Send(p, 3)
	sender.Send(p, 4)
	sender.Send(p, 5)
	sender.Send(p, 6)

	as.Equal(18, receiver.MustReceive(c))
	as.Equal(36, receiver.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic()
	out := essentials.NewTopic()

	s, err := build.
		TopicSource(in).
		ReduceFrom(func(l event.Event, r event.Event) event.Event {
			return l.(int) + r.(int)
		}, 10).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	sender.Send(p, 1)
	sender.Send(p, 2)
	sender.Send(p, 3)

	as.Equal(11, receiver.MustReceive(c))
	as.Equal(13, receiver.MustReceive(c))
	as.Equal(16, receiver.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestProcessorFunc(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic()
	out := essentials.NewTopic()

	s, err := build.
		TopicSource(in).
		ProcessorFunc(func(_ event.Event, r stream.Reporter) {
			r.Result(42)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	sender.Send(p, 1)
	sender.Send(p, 2)

	as.Equal(42, receiver.MustReceive(c))
	as.Equal(42, receiver.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestMerge(t *testing.T) {
	as := assert.New(t)

	l := essentials.NewTopic()
	r := essentials.NewTopic()
	out := essentials.NewTopic()

	s, err := build.
		TopicSource(l).
		ProcessorFunc(func(_ event.Event, r stream.Reporter) {
			r.Result(42)
		}).
		Merge(
			build.
				TopicSource(r).
				ProcessorFunc(func(_ event.Event, r stream.Reporter) {
					r.Result(96)
				}),
		).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	lp := l.NewProducer()
	rp := r.NewProducer()
	c := out.NewConsumer()

	sender.Send(lp, 1)
	sender.Send(rp, 2)
	sender.Send(lp, 10001)
	sender.Send(rp, 1234)

	is42Or96 := func(e event.Event) bool {
		return e.(int) == 42 || e.(int) == 96
	}

	as.True(is42Or96(receiver.MustReceive(c)))
	as.True(is42Or96(receiver.MustReceive(c)))
	as.True(is42Or96(receiver.MustReceive(c)))
	as.True(is42Or96(receiver.MustReceive(c)))

	lp.Close()
	rp.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestMergeBuildError(t *testing.T) {
	as := assert.New(t)

	in := essentials.NewTopic()

	s, err := build.
		Merge(
			build.TopicSource(in).Deferred(func() (stream.Processor, error) {
				return nil, errors.New("error raised")
			}),
		).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}

func TestJoin(t *testing.T) {
	as := assert.New(t)

	l := essentials.NewTopic()
	r := essentials.NewTopic()
	out := essentials.NewTopic()

	s, err := build.
		TopicSource(l).
		Join(
			build.TopicSource(r),
			func(l event.Event, r event.Event) bool {
				return true
			},
			func(l event.Event, r event.Event) event.Event {
				return l.(int) + r.(int)
			},
		).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	lp := l.NewProducer()
	rp := r.NewProducer()
	c := out.NewConsumer()

	sender.Send(lp, 1)
	sender.Send(rp, 2)
	sender.Send(rp, 1234)
	sender.Send(lp, 10001)

	as.Equal(3, receiver.MustReceive(c))
	as.Equal(11235, receiver.MustReceive(c))

	lp.Close()
	rp.Close()
	c.Close()
	as.Nil(s.Stop())
}

type row struct {
	id   id.ID
	name string
	age  int
}

func TestTableSink(t *testing.T) {
	as := assert.New(t)

	in := essentials.NewTopic()
	out := streaming.NewTable(
		func(e event.Event) (table.Key, error) {
			return e.(*row).id, nil
		},
		column.Make("age", func(e event.Event) (table.Value, error) {
			return e.(*row).age, nil
		}),
	)

	s, err := build.
		TopicSource(in).
		TableSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	billID := id.New()
	sender.Send(p, &row{
		id:   billID,
		name: "bill",
		age:  42,
	})
	p.Close()

	time.Sleep(50 * time.Millisecond)
	sel, err := out.Selector("age")
	as.NotNil(sel)
	as.Nil(err)

	res, err := sel(billID)
	as.Nil(err)
	as.Equal(42, res[0])
}

func TestTableLookup(t *testing.T) {
	as := assert.New(t)

	theID := id.New()
	ks := func(_ event.Event) (table.Key, error) {
		return theID, nil
	}

	in := essentials.NewTopic()
	tbl := streaming.NewTable(ks,
		column.Make("*", func(e event.Event) (table.Value, error) {
			return e, nil
		}),
	)
	res, err := tbl.Update("hello there")
	as.Equal(table.Relation{"hello there"}, res)
	as.Nil(err)
	out := essentials.NewTopic()

	s, err := build.
		TopicSource(in).
		TableLookup(tbl, "*", ks).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	sender.Send(p, "anything")
	p.Close()

	c := out.NewConsumer()
	as.Equal("hello there", receiver.MustReceive(c))
	c.Close()
}

func TestJoinBuildError(t *testing.T) {
	as := assert.New(t)

	l := essentials.NewTopic()
	r := essentials.NewTopic()

	s, err := build.
		Join(
			build.TopicSource(l).Deferred(func() (stream.Processor, error) {
				return nil, errors.New("error raised")
			}),
			build.TopicSource(r),
			func(l event.Event, r event.Event) bool {
				return true
			},
			func(l event.Event, r event.Event) event.Event {
				return l.(int) + r.(int)
			},
		).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}

func TestDeferredError(t *testing.T) {
	as := assert.New(t)

	in := essentials.NewTopic()
	s, err := build.
		TopicSource(in).
		Deferred(func() (stream.Processor, error) {
			return nil, errors.New("error raised")
		}).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}
