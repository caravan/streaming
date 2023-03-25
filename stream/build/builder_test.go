package build_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials/id"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/build"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"
)

func TestPump(t *testing.T) {
	as := assert.New(t)
	in := topic.New()
	out := topic.New()

	s, err := build.TopicSource(in).TopicSink(out).Stream()
	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	topic.Send(p, 1)
	topic.Send(p, 2)
	topic.Send(p, 3)

	as.Equal(1, topic.MustReceive(c))
	as.Equal(2, topic.MustReceive(c))
	as.Equal(3, topic.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestFilterMapReduce(t *testing.T) {
	as := assert.New(t)
	in := topic.New()
	out := topic.New()

	s, err := build.
		TopicSource(in).
		Filter(func(e stream.Event) bool {
			return e.(int)%2 == 0
		}).
		Map(func(e stream.Event) stream.Event {
			return e.(int) * 3
		}).
		Reduce(func(l stream.Event, r stream.Event) stream.Event {
			return l.(int) + r.(int)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	topic.Send(p, 1)
	topic.Send(p, 2)
	topic.Send(p, 3)
	topic.Send(p, 4)
	topic.Send(p, 5)
	topic.Send(p, 6)

	as.Equal(18, topic.MustReceive(c))
	as.Equal(36, topic.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)
	in := topic.New()
	out := topic.New()

	s, err := build.
		TopicSource(in).
		ReduceFrom(func(l stream.Event, r stream.Event) stream.Event {
			return l.(int) + r.(int)
		}, 10).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	topic.Send(p, 1)
	topic.Send(p, 2)
	topic.Send(p, 3)

	as.Equal(11, topic.MustReceive(c))
	as.Equal(13, topic.MustReceive(c))
	as.Equal(16, topic.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestProcessorFunc(t *testing.T) {
	as := assert.New(t)
	in := topic.New()
	out := topic.New()

	s, err := build.
		TopicSource(in).
		ProcessorFunc(func(_ stream.Event, r stream.Reporter) {
			r.Result(42)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	topic.Send(p, 1)
	topic.Send(p, 2)

	as.Equal(42, topic.MustReceive(c))
	as.Equal(42, topic.MustReceive(c))

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestMerge(t *testing.T) {
	as := assert.New(t)

	l := topic.New()
	r := topic.New()
	out := topic.New()

	s, err := build.
		TopicSource(l).
		ProcessorFunc(func(_ stream.Event, r stream.Reporter) {
			r.Result(42)
		}).
		Merge(
			build.
				TopicSource(r).
				ProcessorFunc(func(_ stream.Event, r stream.Reporter) {
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

	topic.Send(lp, 1)
	topic.Send(rp, 2)
	topic.Send(lp, 10001)
	topic.Send(rp, 1234)

	is42Or96 := func(e stream.Event) bool {
		return e.(int) == 42 || e.(int) == 96
	}

	as.True(is42Or96(topic.MustReceive(c)))
	as.True(is42Or96(topic.MustReceive(c)))
	as.True(is42Or96(topic.MustReceive(c)))
	as.True(is42Or96(topic.MustReceive(c)))

	lp.Close()
	rp.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestMergeBuildError(t *testing.T) {
	as := assert.New(t)

	in := topic.New()

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

	l := topic.New()
	r := topic.New()
	out := topic.New()

	s, err := build.
		TopicSource(l).
		Join(
			build.TopicSource(r),
			func(l stream.Event, r stream.Event) bool {
				return true
			},
			func(l stream.Event, r stream.Event) stream.Event {
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

	topic.Send(lp, 1)
	topic.Send(rp, 2)
	topic.Send(rp, 1234)
	topic.Send(lp, 10001)

	as.Equal(3, topic.MustReceive(c))
	as.Equal(11235, topic.MustReceive(c))

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

	in := topic.New()
	out := streaming.NewTable(
		func(e stream.Event) (table.Key, error) {
			return e.(*row).id, nil
		},
		column.Make("age", func(e stream.Event) (table.Value, error) {
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
	topic.Send(p, &row{
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
	ks := func(_ stream.Event) (table.Key, error) {
		return theID, nil
	}

	in := topic.New()
	tbl := streaming.NewTable(ks,
		column.Make("*", func(e stream.Event) (table.Value, error) {
			return e, nil
		}),
	)
	res, err := tbl.Update("hello there")
	as.Equal(table.Relation{"hello there"}, res)
	as.Nil(err)
	out := topic.New()

	s, err := build.
		TopicSource(in).
		TableLookup(tbl, "*", ks).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	topic.Send(p, "anything")
	p.Close()

	c := out.NewConsumer()
	as.Equal("hello there", topic.MustReceive(c))
	c.Close()
}

func TestJoinBuildError(t *testing.T) {
	as := assert.New(t)

	l := topic.New()
	r := topic.New()

	s, err := build.
		Join(
			build.TopicSource(l).Deferred(func() (stream.Processor, error) {
				return nil, errors.New("error raised")
			}),
			build.TopicSource(r),
			func(l stream.Event, r stream.Event) bool {
				return true
			},
			func(l stream.Event, r stream.Event) stream.Event {
				return l.(int) + r.(int)
			},
		).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}

func TestDeferredError(t *testing.T) {
	as := assert.New(t)

	in := topic.New()
	s, err := build.
		TopicSource(in).
		Deferred(func() (stream.Processor, error) {
			return nil, errors.New("error raised")
		}).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}
