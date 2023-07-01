package context_test

import (
	"errors"
	"testing"

	"github.com/caravan/streaming/stream/context"
	"github.com/stretchr/testify/assert"
)

func TestOpenContext(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	err := make(chan error)
	in := make(chan any)
	out := make(chan any)

	c := context.Make(done, err, in, out)
	as.NotNil(c)
	as.False(c.IsDone())

	go func() { in <- "hello" }()
	msg, ok := c.FetchMessage()
	as.True(ok)
	as.Equal("hello", msg)

	go func() {
		ok = c.ForwardResult("goodbye")
		as.True(ok)
	}()
	as.Equal("goodbye", <-out)

	go func() {
		ok = c.ReportError(errors.New("hello"))
		as.True(ok)
	}()
	as.EqualError(<-err, "hello")
}

func TestClosedContext(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	close(done)

	err := make(chan error)
	in := make(chan any)
	out := make(chan any)

	c := context.Make(done, err, in, out)
	as.NotNil(c)
	as.True(c.IsDone())

	go func() { in <- "hello" }()
	msg, ok := c.FetchMessage()
	as.False(ok)
	as.Equal(nil, msg)

	ok = c.ForwardResult("goodbye")
	as.False(ok)

	ok = c.ReportError(errors.New("hello"))
	as.False(ok)
}

func TestContextWith(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	err := make(chan error)

	c1 := context.Make[any, any](done, err, nil, nil)
	as.Nil(c1.In)
	as.Nil(c1.Out)

	c2 := context.WithIn(c1, make(chan any))
	as.Nil(c2.Out)
	as.NotNil(c2.In)
	as.Nil(c1.In)

	c3 := context.WithOut(c2, make(chan any))
	as.NotNil(c3.Out)
	as.NotNil(c3.In)
	as.Nil(c2.Out)
	as.Equal(c3.In, c2.In)

	c4 := context.With(c3, make(chan any), make(chan any))
	as.NotNil(c4.Out)
	as.NotNil(c4.Out)
	as.NotEqual(c3.Out, c4.Out)
	as.NotEqual(c3.In, c4.In)
}
