package node

import (
	"github.com/caravan/essentials/event"
	"github.com/caravan/streaming/stream"
)

type forward struct{}

// Forward is a processor that forwards the provided Event as its Result
var Forward = forward{}

func (forward) Process(e event.Event, r stream.Reporter) {
	r.Result(e)
}
