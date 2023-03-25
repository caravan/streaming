package node

import (
	"runtime"

	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
)

type (
	topicSource struct {
		topic.Consumer[stream.Event]
	}

	topicSink struct {
		topic.Producer[stream.Event]
	}
)

// TopicSource constructs a processor that receives from the provided Topic
// every time it's invoked by the Stream
func TopicSource(t topic.Topic[stream.Event]) stream.SourceProcessor {
	res := &topicSource{
		Consumer: t.NewConsumer(),
	}
	runtime.SetFinalizer(res, func(s *topicSource) {
		s.Close()
	})
	return res
}

func (*topicSource) Source() {}

func (s *topicSource) Process(_ stream.Event, r stream.Reporter) {
	e := <-s.Receive() // our Consumer, won't close
	r.Result(e)
}

// TopicSink constructs a processor that sends all Events it sees to the
// provided Topic
func TopicSink(t topic.Topic[stream.Event]) stream.SinkProcessor {
	res := &topicSink{
		Producer: t.NewProducer(),
	}
	runtime.SetFinalizer(res, func(s *topicSink) {
		s.Close()
	})
	return res
}

func (*topicSink) Sink() {}

func (s *topicSink) Process(e stream.Event, _ stream.Reporter) {
	s.Send() <- e
}
