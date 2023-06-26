package node_test

import (
	"testing"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestForward(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan int)
	out := make(chan int)

	var p stream.Processor[int, int] = node.Forward[int]
	p.Start(context.Make(done, make(chan error), in, out))

	in <- 42
	as.Equal(42, <-out)
	close(done)
}
