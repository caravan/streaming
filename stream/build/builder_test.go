package build_test

import (
	"errors"
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/build"
	"github.com/stretchr/testify/assert"
)

func TestPump(t *testing.T) {
	as := assert.New(t)
	in := essentials.NewTopic[int]()
	out := essentials.NewTopic[int]()

	s, err := build.TopicSource[int](in).TopicSink(out).Stream()
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
		TopicSource[int](in).
		Filter(func(e int) bool {
			return e%2 == 0
		}).
		Map(func(e int) int {
			return e * 3
		}).
		Reduce(func(l int, r int) int {
			return l + r
		}).
		TopicSink(out).
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
		TopicSource[int](in).
		ReduceFrom(func(l int, r int) int {
			return l + r
		}, 10).
		TopicSink(out).
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
		TopicSource[int](in).
		Processor(func(_ int, r stream.Reporter[int]) {
			r(42, nil)
		}).
		TopicSink(out).
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
		TopicSource[int](l).
		Processor(func(_ int, r stream.Reporter[int]) {
			r(42, nil)
		}).
		Merge(
			build.
				TopicSource[int](r).
				Processor(func(_ int, r stream.Reporter[int]) {
					r(96, nil)
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

	lp.Send() <- 1
	rp.Send() <- 2
	lp.Send() <- 10001
	rp.Send() <- 1234

	is42Or96 := func(e int) bool {
		return e == 42 || e == 96
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
			build.TopicSource[any](in).Deferred(
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
		TopicSource[int](l).
		Join(
			build.TopicSource[int](r),
			func(l int, r int) bool {
				return true
			},
			func(l int, r int) int {
				return l + r
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

func TestJoinBuildError(t *testing.T) {
	as := assert.New(t)

	l := essentials.NewTopic[int]()
	r := essentials.NewTopic[int]()

	s, err := build.
		Join(
			build.TopicSource[int](l).Deferred(
				func() (stream.Processor[int, int], error) {
					return nil, errors.New("error raised")
				},
			),
			build.TopicSource[int](r),
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
		TopicSource[any](in).
		Deferred(func() (stream.Processor[any, any], error) {
			return nil, errors.New("error raised")
		}).
		Stream()

	as.Nil(s)
	as.EqualError(err, "error raised")
}
