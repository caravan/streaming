package reporter_test

import (
	"errors"
	"testing"

	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/caravan/streaming/stream"
	"github.com/stretchr/testify/assert"
)

type wrappable struct {
	*reporter.Reporter
}

func TestWrap(t *testing.T) {
	as := assert.New(t)

	r := reporter.Make(
		func(e stream.Event) {
			as.Equal("hello", e)
		},
		func(e error) {
			as.EqualError(e, "explosion")
		},
	)

	as.Equal(r, reporter.Wrap(r))
	r.Result("hello")
	r.Error(errors.New("explosion"))

	w := reporter.Wrap(&wrappable{
		Reporter: r,
	})

	as.NotEqual(r, w)
	w.Result("hello")
	w.Error(errors.New("explosion"))
}

func TestWith(t *testing.T) {
	as := assert.New(t)

	r := (&reporter.Reporter{}).WithResult(
		func(e stream.Event) {
			as.Equal("hello", e)
		},
	).WithError(
		func(e error) {
			as.EqualError(e, "explosion")
		},
	)

	r.Result("hello")
	r.Error(errors.New("explosion"))
}
