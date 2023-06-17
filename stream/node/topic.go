package node

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
)

// TopicSource constructs a processor that receives from the provided Topic
// every time it's invoked by the Stream
func TopicSource[Msg any](t topic.Topic[Msg]) stream.Processor[any, Msg] {
	c := t.NewConsumer()
	return func(_ any, rep stream.Reporter[Msg]) {
		if m, ok := <-c.Receive(); ok {
			rep(m, nil)
		}
	}
}

// TopicSink constructs a processor that sends all messages it sees to the
// provided Topic
func TopicSink[Msg any](t topic.Topic[Msg]) stream.Processor[Msg, Msg] {
	p := t.NewProducer()
	return func(msg Msg, rep stream.Reporter[Msg]) {
		p.Send() <- msg
		Forward(msg, rep)
	}
}
