package build_test

import (
	"errors"
	"testing"
	"time"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/build"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"
)

func TestPump(t *testing.T) {
	as := assert.New(t)
	in := caravan.NewTopic()
	out := caravan.NewTopic()

	s, err := build.TopicSource(in).TopicSink(out).Stream()
	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send(1)
	p.Send(2)
	p.Send(3)

	as.Equal(1, topic.MustReceive(c))
	as.Equal(2, topic.MustReceive(c))
	as.Equal(3, topic.MustReceive(c))

	_ = p.Close()
	_ = c.Close()
	as.Nil(s.Stop())
}

func TestFilterMapReduce(t *testing.T) {
	as := assert.New(t)
	in := caravan.NewTopic()
	out := caravan.NewTopic()

	s, err := build.
		TopicSource(in).
		Filter(func(e topic.Event) bool {
			return e.(int)%2 == 0
		}).
		Map(func(e topic.Event) topic.Event {
			return e.(int) * 3
		}).
		Reduce(func(l topic.Event, r topic.Event) topic.Event {
			return l.(int) + r.(int)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send(1)
	p.Send(2)
	p.Send(3)
	p.Send(4)
	p.Send(5)
	p.Send(6)

	as.Equal(18, topic.MustReceive(c))
	as.Equal(36, topic.MustReceive(c))

	_ = p.Close()
	_ = c.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)
	in := caravan.NewTopic()
	out := caravan.NewTopic()

	s, err := build.
		TopicSource(in).
		ReduceFrom(func(l topic.Event, r topic.Event) topic.Event {
			return l.(int) + r.(int)
		}, 10).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send(1)
	p.Send(2)
	p.Send(3)

	as.Equal(11, topic.MustReceive(c))
	as.Equal(13, topic.MustReceive(c))
	as.Equal(16, topic.MustReceive(c))

	_ = p.Close()
	_ = c.Close()
	as.Nil(s.Stop())
}

func TestProcessorFunc(t *testing.T) {
	as := assert.New(t)
	in := caravan.NewTopic()
	out := caravan.NewTopic()

	s, err := build.
		TopicSource(in).
		ProcessorFunc(func(_ topic.Event, r stream.Reporter) {
			r.Result(42)
		}).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send(1)
	p.Send(2)

	as.Equal(42, topic.MustReceive(c))
	as.Equal(42, topic.MustReceive(c))

	_ = p.Close()
	_ = c.Close()
	as.Nil(s.Stop())
}

func TestMerge(t *testing.T) {
	as := assert.New(t)

	l := caravan.NewTopic()
	r := caravan.NewTopic()
	out := caravan.NewTopic()

	s, err := build.
		TopicSource(l).
		ProcessorFunc(func(_ topic.Event, r stream.Reporter) {
			r.Result(42)
		}).
		Merge(
			build.
				TopicSource(r).
				ProcessorFunc(func(_ topic.Event, r stream.Reporter) {
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

	lp.Send(1)
	rp.Send(2)
	lp.Send(10001)
	rp.Send(1234)

	is42Or96 := func(e topic.Event) bool {
		return e.(int) == 42 || e.(int) == 96
	}

	as.True(is42Or96(topic.MustReceive(c)))
	as.True(is42Or96(topic.MustReceive(c)))
	as.True(is42Or96(topic.MustReceive(c)))
	as.True(is42Or96(topic.MustReceive(c)))

	_ = lp.Close()
	_ = rp.Close()
	_ = c.Close()
	as.Nil(s.Stop())
}

func TestMergeBuildError(t *testing.T) {
	as := assert.New(t)

	in := caravan.NewTopic()

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

	l := caravan.NewTopic()
	r := caravan.NewTopic()
	out := caravan.NewTopic()

	s, err := build.
		TopicSource(l).
		Join(
			build.TopicSource(r),
			func(l topic.Event, r topic.Event) bool {
				return true
			},
			func(l topic.Event, r topic.Event) topic.Event {
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

	lp.Send(1)
	rp.Send(2)
	rp.Send(1234)
	lp.Send(10001)

	as.Equal(3, topic.MustReceive(c))
	as.Equal(11235, topic.MustReceive(c))

	_ = lp.Close()
	_ = rp.Close()
	_ = c.Close()
	as.Nil(s.Stop())
}

type row struct {
	id   id.ID
	name string
	age  int
}

func TestTableSink(t *testing.T) {
	as := assert.New(t)

	in := caravan.NewTopic()
	out := streaming.NewTable(
		func(e topic.Event) (table.Key, error) {
			return e.(*row).id, nil
		},
		column.Make("age", func(e topic.Event) (table.Value, error) {
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
	p.Send(&row{
		id:   billID,
		name: "bill",
		age:  42,
	})
	_ = p.Close()

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
	ks := func(_ topic.Event) (table.Key, error) {
		return theID, nil
	}

	in := caravan.NewTopic()
	tbl := streaming.NewTable(ks,
		column.Make("*", func(e topic.Event) (table.Value, error) {
			return e, nil
		}),
	)
	res, err := tbl.Update("hello there")
	as.Equal(table.Relation{"hello there"}, res)
	as.Nil(err)
	out := caravan.NewTopic()

	s, err := build.
		TopicSource(in).
		TableLookup(tbl, "*", ks).
		TopicSink(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	p.Send("anything")
	_ = p.Close()

	c := out.NewConsumer()
	as.Equal("hello there", topic.MustReceive(c))
	_ = c.Close()
}

func TestJoinBuildError(t *testing.T) {
	as := assert.New(t)

	l := caravan.NewTopic()
	r := caravan.NewTopic()

	s, err := build.
		Join(
			build.TopicSource(l).Deferred(func() (stream.Processor, error) {
				return nil, errors.New("error raised")
			}),
			build.TopicSource(r),
			func(l topic.Event, r topic.Event) bool {
				return true
			},
			func(l topic.Event, r topic.Event) topic.Event {
				return l.(int) + r.(int)
			},
		).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}

func TestDeferredError(t *testing.T) {
	as := assert.New(t)

	in := caravan.NewTopic()
	s, err := build.
		TopicSource(in).
		Deferred(func() (stream.Processor, error) {
			return nil, errors.New("error raised")
		}).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}
