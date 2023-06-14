package node

import "github.com/caravan/streaming/stream"

type Forward[Msg any] struct{}

func (Forward[Msg]) Process(m Msg, r stream.Reporter[Msg]) {
	r.Result(m)
}
