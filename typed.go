package streaming

import (
	"github.com/caravan/streaming/internal/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"

	_stream "github.com/caravan/streaming/internal/stream"
)

type (
	Typed[Msg any] interface {
		NewStream(p ...stream.Processor[Msg]) stream.Stream
		ProcessorFunc(func(Msg, stream.Reporter[Msg])) stream.ProcessorFunc[Msg]
		Filter(node.Predicate[Msg]) stream.Processor[Msg]
		ForEach(node.ForEachFunc[Msg]) stream.Processor[Msg]
		Join(left stream.Processor[Msg], right stream.Processor[Msg],
			predicate node.BinaryPredicate[Msg], joiner node.Joiner[Msg],
		) stream.SourceProcessor[Msg]
		Map(node.Mapper[Msg]) stream.Processor[Msg]
		Merge(p ...stream.Processor[Msg]) stream.SourceProcessor[Msg]
		Reduce(node.Reducer[Msg]) stream.Processor[Msg]
		ReduceFrom(node.Reducer[Msg], Msg) stream.Processor[Msg]
		Subprocess(p ...stream.Processor[Msg]) stream.Processor[Msg]
		TopicSource(topic.Topic[Msg]) stream.SourceProcessor[Msg]
		TopicSink(topic.Topic[Msg]) stream.SinkProcessor[Msg]
	}

	typed[Msg any] struct{}
)

func Of[Msg any]() Typed[Msg] {
	return typed[Msg]{}
}

func (t typed[Msg]) NewStream(p ...stream.Processor[Msg]) stream.Stream {
	return _stream.Make[Msg](p...)
}

func (t typed[Msg]) ProcessorFunc(
	p func(Msg, stream.Reporter[Msg]),
) stream.ProcessorFunc[Msg] {
	return p
}

func (t typed[Msg]) Filter(n node.Predicate[Msg]) stream.Processor[Msg] {
	return node.Filter[Msg](n)
}

func (t typed[Msg]) ForEach(n node.ForEachFunc[Msg]) stream.Processor[Msg] {
	return node.ForEach[Msg](n)
}

func (t typed[Msg]) Join(
	left stream.Processor[Msg], right stream.Processor[Msg],
	predicate node.BinaryPredicate[Msg], joiner node.Joiner[Msg],
) stream.SourceProcessor[Msg] {
	return node.Join[Msg](left, right, predicate, joiner)
}

func (t typed[Msg]) Map(n node.Mapper[Msg]) stream.Processor[Msg] {
	return node.Map[Msg](n)
}

func (t typed[Msg]) Merge(
	p ...stream.Processor[Msg],
) stream.SourceProcessor[Msg] {
	return node.Merge[Msg](p...)
}

func (t typed[Msg]) Reduce(n node.Reducer[Msg]) stream.Processor[Msg] {
	return node.Reduce[Msg](n)
}

func (t typed[Msg]) ReduceFrom(
	n node.Reducer[Msg], init Msg,
) stream.Processor[Msg] {
	return node.ReduceFrom[Msg](n, init)
}

func (t typed[Msg]) Subprocess(
	p ...stream.Processor[Msg],
) stream.Processor[Msg] {
	return node.Subprocess[Msg](p...)
}

func (t typed[Msg]) TopicSource(
	top topic.Topic[Msg],
) stream.SourceProcessor[Msg] {
	return node.TopicSource[Msg](top)
}

func (t typed[Msg]) TopicSink(top topic.Topic[Msg]) stream.SinkProcessor[Msg] {
	return node.TopicSink[Msg](top)
}
