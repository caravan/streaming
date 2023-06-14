package node

import (
	"sync"

	"github.com/caravan/streaming/stream"
)

type merge[Msg any] []stream.Processor[Msg]

// Merge forwards Events from multiple Processors to the same Reporter
func Merge[Msg any](p ...stream.Processor[Msg]) stream.SourceProcessor[Msg] {
	return merge[Msg](p)
}

func (m merge[_]) Source() {}

func (m merge[Msg]) Process(msg Msg, r stream.Reporter[Msg]) {
	var group sync.WaitGroup
	group.Add(len(m))

	for _, p := range m {
		go func(p stream.Processor[Msg]) {
			p.Process(msg, r)
			group.Done()
		}(p)
	}

	group.Wait()
}
