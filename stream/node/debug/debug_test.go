package debug_test

import (
	"testing"

	_debug "github.com/caravan/essentials/debug"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node/debug"
	"github.com/stretchr/testify/assert"
)

func TestProcessorReturnedEarly(t *testing.T) {
	as := assert.New(t)

	m := debug.ProcessorReturnedEarly(func(c *context.Context[any, any]) {
		// no need to do anything
	})

	monitor := make(chan context.Advice)
	c := context.Make(
		make(chan context.Done), monitor, make(chan any), make(chan any),
	)
	m.Start(c)

	if _debug.IsEnabled() {
		as.EqualError(
			(<-monitor).(*context.Fatal),
			debug.ErrProcessorReturnedEarly,
		)
	}
}
