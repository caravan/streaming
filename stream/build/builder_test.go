package build_test

import (
	"errors"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/id"
	"github.com/caravan/streaming"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/build"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"
	"github.com/caravan/streaming/table/column"
	"github.com/stretchr/testify/assert"
)

func makeNumberProcessor(val int) stream.Processor[int, int] {
	return node.Map(func(i int) int {
		return val
	})
}

func TestPump(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic[int]()
	out := essentials.NewTopic[int]()

	s, err := build.TopicConsumer(in).TopicProducer(out).Stream()
	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	as.Equal(1, <-c.Receive())
	as.Equal(2, <-c.Receive())
	as.Equal(3, <-c.Receive())

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestFilterMapReduce(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic[int]()
	out := essentials.NewTopic[int]()

	s, err := build.
		TopicConsumer(in).
		Filter(func(i int) bool {
			return i%2 == 0
		}).
		Map(func(i int) int {
			return i * 3
		}).
		Reduce(func(l int, r int) int {
			return l + r
		}).
		TopicProducer(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3
	p.Send() <- 4
	p.Send() <- 5
	p.Send() <- 6

	as.Equal(18, <-c.Receive())
	as.Equal(36, <-c.Receive())

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic[int]()
	out := essentials.NewTopic[int]()

	s, err := build.
		TopicConsumer(in).
		ReduceFrom(func(l int, r int) int {
			return l + r
		}, 10).
		TopicProducer(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	as.Equal(11, <-c.Receive())
	as.Equal(13, <-c.Receive())
	as.Equal(16, <-c.Receive())

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestProcessor(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic[int]()
	out := essentials.NewTopic[int]()

	s, err := build.
		TopicConsumer(in).
		Processor(makeNumberProcessor(42)).
		TopicProducer(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	c := out.NewConsumer()

	p.Send() <- 1
	p.Send() <- 2

	as.Equal(42, <-c.Receive())
	as.Equal(42, <-c.Receive())

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestMerge(t *testing.T) {
	as := assert.New(t)

	l := essentials.NewTopic[int]()
	r := essentials.NewTopic[int]()
	out := essentials.NewTopic[int]()

	s, err := build.
		TopicConsumer(l).
		Processor(makeNumberProcessor(42)).
		Merge(
			build.
				TopicConsumer(r).
				Processor(makeNumberProcessor(96)),
		).
		TopicProducer(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	lp := l.NewProducer()
	rp := r.NewProducer()
	c := out.NewConsumer()

	lp.Send() <- 1
	rp.Send() <- 2
	lp.Send() <- 10001
	rp.Send() <- 1234

	is42Or96 := func(i int) bool {
		return i == 42 || i == 96
	}

	as.True(is42Or96(<-c.Receive()))
	as.True(is42Or96(<-c.Receive()))
	as.True(is42Or96(<-c.Receive()))
	as.True(is42Or96(<-c.Receive()))

	lp.Close()
	rp.Close()
	c.Close()
	as.Nil(s.Stop())
}

func TestMergeBuildError(t *testing.T) {
	as := assert.New(t)

	in := essentials.NewTopic[any]()

	s, err := build.
		Merge(
			build.TopicConsumer(in).Deferred(
				func() (stream.Processor[any, any], error) {
					return nil, errors.New("error raised")
				},
			),
		).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}

func TestJoin(t *testing.T) {
	as := assert.New(t)

	l := essentials.NewTopic[int]()
	r := essentials.NewTopic[int]()
	out := essentials.NewTopic[int]()

	s, err := build.
		TopicConsumer(l).
		Join(
			build.TopicConsumer(r),
			func(l int, r int) bool {
				return true
			},
			func(l int, r int) int {
				return l + r
			},
		).
		TopicProducer(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	lp := l.NewProducer()
	rp := r.NewProducer()
	c := out.NewConsumer()

	lp.Send() <- 1
	rp.Send() <- 2
	rp.Send() <- 1234
	lp.Send() <- 10001

	as.Equal(3, <-c.Receive())
	as.Equal(11235, <-c.Receive())

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

	in := essentials.NewTopic[any]()
	out := streaming.NewTable[any](
		func(a any) (table.Key, error) {
			return a.(*row).id, nil
		},
		column.Make("age", func(a any) (any, error) {
			return a.(*row).age, nil
		}),
	)

	s, err := build.
		TopicConsumer(in).
		TableUpdater(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	billID := id.New()
	p.Send() <- &row{
		id:   billID,
		name: "bill",
		age:  42,
	}
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
	ks := func(_ any) (table.Key, error) {
		return theID, nil
	}

	in := essentials.NewTopic[any]()
	tbl := streaming.NewTable[any](ks,
		column.Make("*", func(a any) (any, error) {
			return a, nil
		}),
	)
	res, err := tbl.Update("hello there")
	as.Equal(table.Relation[any]{"hello there"}, res)
	as.Nil(err)
	out := essentials.NewTopic[any]()

	s, err := build.
		TopicConsumer(in).
		TableLookup(tbl, "*", ks).
		TopicProducer(out).
		Stream()

	as.NotNil(s)
	as.Nil(err)
	as.Nil(s.Start())

	p := in.NewProducer()
	p.Send() <- "anything"
	p.Close()

	c := out.NewConsumer()
	as.Equal("hello there", <-c.Receive())
	c.Close()
}

func TestJoinBuildError(t *testing.T) {
	as := assert.New(t)

	l := essentials.NewTopic[int]()
	r := essentials.NewTopic[int]()

	s, err := build.
		Join(
			build.TopicConsumer(l).Deferred(
				func() (stream.Processor[int, int], error) {
					return nil, errors.New("error raised")
				},
			),
			build.TopicConsumer(r),
			func(l int, r int) bool {
				return true
			},
			func(l int, r int) int {
				return l + r
			},
		).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}

func TestDeferredError(t *testing.T) {
	as := assert.New(t)

	in := essentials.NewTopic[any]()
	s, err := build.
		TopicConsumer(in).
		Deferred(func() (stream.Processor[any, any], error) {
			return nil, errors.New("error raised")
		}).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}
