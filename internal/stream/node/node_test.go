package node_test

import (
	"testing"

	"github.com/caravan/streaming/stream"

	"github.com/caravan/streaming/internal/stream/node"
	"github.com/caravan/streaming/internal/stream/reporter"
	"github.com/stretchr/testify/assert"
)

func TestForward(t *testing.T) {
	as := assert.New(t)
	node.Forward.Process(42,
		reporter.Make(
			func(e stream.Event) {
				as.Equal(42, e)
			},
			func(err error) {
				as.Fail("should not be called")
			},
		),
	)
}
