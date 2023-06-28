package node

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/context"
)

// TopicConsumer constructs a processor that receives from the provided Topic
// every time it's invoked by the Stream
func TopicConsumer[Msg any](
	t topic.Topic[Msg],
) stream.Processor[stream.Source, Msg] {
	consumer := t.NewConsumer().Receive()
	return func(c *context.Context[stream.Source, Msg]) {
		fetchFromTopic := func() (Msg, bool) {
			select {
			case <-c.Done:
				var zero Msg
				return zero, false
			case msg, ok := <-consumer:
				return msg, ok
			}
		}

		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			} else if msg, ok := fetchFromTopic(); !ok {
				return
			} else if !c.ForwardResult(msg) {
				return
			}
		}
	}
}

// TopicProducer constructs a processor that sends all messages it sees to the
// provided Topic
func TopicProducer[Msg any](t topic.Topic[Msg]) stream.Processor[Msg, Msg] {
	producer := t.NewProducer().Send()
	return func(c *context.Context[Msg, Msg]) {
		forwardToTopic := func(msg Msg) bool {
			select {
			case <-c.Done:
				return false
			case producer <- msg:
				return true
			}
		}

		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if !forwardToTopic(msg) {
				return
			} else if !c.ForwardResult(msg) {
				return
			}
		}
	}
}
