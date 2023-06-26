package node_test

import (
	"testing"

	"github.com/caravan/streaming/stream/node"

	"github.com/caravan/streaming/stream/context"
	"github.com/stretchr/testify/assert"
)

func TestGenerate(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan int)
	out := make(chan int)

	gen := node.Generate(func() int {
		return 42
	})

	gen.Start(context.Make(done, make(chan error), in, out))

	in <- 0
	as.Equal(42, <-out)
	close(done)
}
