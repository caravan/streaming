package node_test

import (
	"testing"

	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestSinkInto(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan int)
	out := make(chan stream.Sink)

	sinkCh := make(chan int)

	sink := node.SinkInto(sinkCh)
	sink.Start(context.Make(done, make(chan error), in, out))

	go func() {
		in <- 42
		in <- 96
	}()

	go func() {
		as.Equal(42, <-sinkCh)
		as.Equal(96, <-sinkCh)
		close(done)
	}()

	<-done
}
