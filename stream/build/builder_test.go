package build_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/message"
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

	message.Send(p, 1)
	message.Send(p, 2)
	message.Send(p, 3)

	as.Equal(1, message.MustReceive(c))
	as.Equal(2, message.MustReceive(c))
	as.Equal(3, message.MustReceive(c))

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
		Filter(func(e message.Event) bool {
			return e.(int)%2 == 0
		}).
		Map(func(e message.Event) message.Event {
			return e.(int) * 3
		}).
		Reduce(func(l message.Event, r message.Event) message.Event {
			return l.(int) + r.(int)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	message.Send(p, 1)
	message.Send(p, 2)
	message.Send(p, 3)
	message.Send(p, 4)
	message.Send(p, 5)
	message.Send(p, 6)

	as.Equal(18, message.MustReceive(c))
	as.Equal(36, message.MustReceive(c))

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
		ReduceFrom(func(l message.Event, r message.Event) message.Event {
			return l.(int) + r.(int)
		}, 10).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	message.Send(p, 1)
	message.Send(p, 2)
	message.Send(p, 3)

	as.Equal(11, message.MustReceive(c))
	as.Equal(13, message.MustReceive(c))
	as.Equal(16, message.MustReceive(c))

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
		ProcessorFunc(func(_ message.Event, r stream.Reporter) {
			r.Result(42)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	message.Send(p, 1)
	message.Send(p, 2)

	as.Equal(42, message.MustReceive(c))
	as.Equal(42, message.MustReceive(c))

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
		ProcessorFunc(func(_ message.Event, r stream.Reporter) {
			r.Result(42)
		}).
		Merge(
			build.
				TopicSource(r).
				ProcessorFunc(func(_ message.Event, r stream.Reporter) {
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

	message.Send(lp, 1)
	message.Send(rp, 2)
	message.Send(lp, 10001)
	message.Send(rp, 1234)

	is42Or96 := func(e message.Event) bool {
		return e.(int) == 42 || e.(int) == 96
	}

	as.True(is42Or96(message.MustReceive(c)))
	as.True(is42Or96(message.MustReceive(c)))
	as.True(is42Or96(message.MustReceive(c)))
	as.True(is42Or96(message.MustReceive(c)))

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
			func(l message.Event, r message.Event) bool {
				return true
			},
			func(l message.Event, r message.Event) message.Event {
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

	message.Send(lp, 1)
	message.Send(rp, 2)
	message.Send(rp, 1234)
	message.Send(lp, 10001)

	as.Equal(3, message.MustReceive(c))
	as.Equal(11235, message.MustReceive(c))

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
		func(e message.Event) (table.Key, error) {
			return e.(*row).id, nil
		},
		column.Make("age", func(e message.Event) (table.Value, error) {
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
	message.Send(p, &row{
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
	ks := func(_ message.Event) (table.Key, error) {
		return theID, nil
	}

	in := essentials.NewTopic()
	tbl := streaming.NewTable(ks,
		column.Make("*", func(e message.Event) (table.Value, error) {
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
	message.Send(p, "anything")
	p.Close()

	c := out.NewConsumer()
	as.Equal("hello there", message.MustReceive(c))
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
			func(l message.Event, r message.Event) bool {
				return true
			},
			func(l message.Event, r message.Event) message.Event {
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
