package node

import (
	"sync"

	"github.com/caravan/streaming/stream"
)

type merge []stream.Processor

// Merge forwards Events from multiple Processors to the same Reporter
func Merge(p ...stream.Processor) stream.SourceProcessor {
	return merge(p)
}

func (m merge) Source() {}

func (m merge) Process(e stream.Event, r stream.Reporter) {
	var group sync.WaitGroup
	group.Add(len(m))

	for _, p := range m {
		go func(p stream.Processor) {
			p.Process(e, r)
			group.Done()
		}(p)
	}

	group.Wait()
}
