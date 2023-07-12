package stream_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials/debug"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/stretchr/testify/assert"
)

func TestProcessorStart(t *testing.T) {
	as := assert.New(t)

	p := stream.Processor[string, string](
		func(c *context.Context[string, string]) {
			// intentionally trip the late return debug
			time.Sleep(10 * time.Millisecond)
		},
	)

	monitor := make(chan context.Advice)
	c := context.Make[string, string](
		make(chan context.Done), monitor, nil, nil,
	)

	p.Start(c)
	if debug.IsEnabled() {
		a, ok := (<-monitor).(*context.Debug)
		as.NotNil(a)
		as.True(ok)
	}
}
