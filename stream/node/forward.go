package node

import "github.com/caravan/streaming/stream"

func Forward[Msg any](msg Msg, rep stream.Reporter[Msg]) {
	rep(msg, nil)
}
