package node

import (
	"github.com/caravan/streaming/internal/stream/node"
	"github.com/caravan/streaming/stream"
)

// Subprocess constructs a processor that consists of the specified Processors,
// each to be invoked one after the other
func Subprocess[Msg any](p ...stream.Processor[Msg]) stream.Processor[Msg] {
	return node.Subprocess[Msg](p...)
}
