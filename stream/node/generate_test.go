package node_test

import (
	"testing"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestGenerate(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan stream.Source)
	out := make(chan int)

	gen := node.Generate(func() (int, bool) {
		return 42, true
	})

	gen.Start(context.Make(done, make(chan error), in, out))

	in <- stream.Source{}
	as.Equal(42, <-out)
	close(done)
}

func TestGenerateFrom(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan stream.Source)
	out := make(chan int)

	genCh := make(chan int)

	gen := node.GenerateFrom(genCh)
	gen.Start(context.Make(done, make(chan error), in, out))

	in <- stream.Source{}
	genCh <- 42
	as.Equal(42, <-out)

	in <- stream.Source{}
	genCh <- 96
	as.Equal(96, <-out)

	close(done)
}
