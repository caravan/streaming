package node

import (
	"runtime"

	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
)

type (
	topicSource[Msg any] struct {
		topic.Consumer[Msg]
	}

	topicSink[Msg any] struct {
		topic.Producer[Msg]
	}
)

// TopicSource constructs a processor that receives from the provided Topic
// every time it's invoked by the Stream
func TopicSource[Msg any](t topic.Topic[Msg]) stream.SourceProcessor[Msg] {
	res := &topicSource[Msg]{
		Consumer: t.NewConsumer(),
	}
	runtime.SetFinalizer(res, func(s *topicSource[Msg]) {
		s.Close()
	})
	return res
}

func (*topicSource[_]) Source() {}

func (s *topicSource[Msg]) Process(_ Msg, r stream.Reporter[Msg]) {
	e := <-s.Receive() // our Consumer, won't close
	r.Result(e)
}

// TopicSink constructs a processor that sends all Events it sees to the
// provided Topic
func TopicSink[Msg any](t topic.Topic[Msg]) stream.SinkProcessor[Msg] {
	res := &topicSink[Msg]{
		Producer: t.NewProducer(),
	}
	runtime.SetFinalizer(res, func(s *topicSink[Msg]) {
		s.Close()
	})
	return res
}

func (*topicSink[_]) Sink() {}

func (s *topicSink[Msg]) Process(m Msg, _ stream.Reporter[Msg]) {
	s.Send() <- m
}
