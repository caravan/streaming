package reporter_test

import (
	"errors"
	"testing"

	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/stretchr/testify/assert"
)

type wrappable[Msg any] struct {
	*reporter.Reporter[Msg]
}

func TestWrap(t *testing.T) {
	as := assert.New(t)

	r := reporter.Make(
		func(e string) {
			as.Equal("hello", e)
		},
		func(e error) {
			as.EqualError(e, "explosion")
		},
	)

	as.Equal(r, reporter.Wrap[string](r))
	r.Result("hello")
	r.Error(errors.New("explosion"))

	w := reporter.Wrap[string](&wrappable[string]{
		Reporter: r,
	})

	as.NotEqual(r, w)
	w.Result("hello")
	w.Error(errors.New("explosion"))
}

func TestWith(t *testing.T) {
	as := assert.New(t)

	r := (&reporter.Reporter[string]{}).WithResult(
		func(m string) {
			as.Equal("hello", m)
		},
	).WithError(
		func(e error) {
			as.EqualError(e, "explosion")
		},
	)

	r.Result("hello")
	r.Error(errors.New("explosion"))
}
