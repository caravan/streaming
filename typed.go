package streaming

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
)

type (
	Typed[Msg any] interface {
		NewStream(...stream.Processor[Msg, Msg]) stream.Stream
		Filter(node.Predicate[Msg]) stream.Processor[Msg, Msg]
		ForEach(node.ForEachFunc[Msg]) stream.Processor[Msg, Msg]
		Join(
			left stream.Processor[Msg, Msg],
			right stream.Processor[Msg, Msg],
			predicate node.BinaryPredicate[Msg, Msg],
			joiner node.BinaryOperator[Msg, Msg, Msg],
		) stream.Processor[Msg, Msg]
		Map(node.Mapper[Msg, Msg]) stream.Processor[Msg, Msg]
		Merge(...stream.Processor[Msg, Msg]) stream.Processor[Msg, Msg]
		ReduceWithReset(
			node.Reducer[Msg, Msg],
		) (stream.Processor[Msg, Msg], node.Reset)
		Reduce(node.Reducer[Msg, Msg]) stream.Processor[Msg, Msg]
		ReduceFromWithReset(
			node.Reducer[Msg, Msg], Msg,
		) (stream.Processor[Msg, Msg], node.Reset)
		ReduceFrom(
			node.Reducer[Msg, Msg], Msg,
		) stream.Processor[Msg, Msg]
		Subprocess(...stream.Processor[Msg, Msg]) stream.Processor[Msg, Msg]
		TopicSource(topic.Topic[Msg]) stream.Processor[Msg, Msg]
		TopicSink(topic.Topic[Msg]) stream.Processor[Msg, Msg]
	}

	typed[Msg any] struct{}
)

func Of[Msg any]() Typed[Msg] {
	return typed[Msg]{}
}

func (t typed[Msg]) NewStream(p ...stream.Processor[Msg, Msg]) stream.Stream {
	return stream.Make[Msg, Msg](
		node.Subprocess(p...),
	)
}

func (t typed[Msg]) Filter(n node.Predicate[Msg]) stream.Processor[Msg, Msg] {
	return node.Filter[Msg](n)
}

func (t typed[Msg]) ForEach(
	n node.ForEachFunc[Msg],
) stream.Processor[Msg, Msg] {
	return node.ForEach[Msg](n)
}

func (t typed[Msg]) Join(
	left stream.Processor[Msg, Msg],
	right stream.Processor[Msg, Msg],
	predicate node.BinaryPredicate[Msg, Msg],
	joiner node.BinaryOperator[Msg, Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Join[Msg](left, right, predicate, joiner)
}

func (t typed[Msg]) Map(n node.Mapper[Msg, Msg]) stream.Processor[Msg, Msg] {
	return node.Map[Msg](n)
}

func (t typed[Msg]) Merge(
	p ...stream.Processor[Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Merge[Msg](p...)
}

func (t typed[Msg]) ReduceWithReset(
	n node.Reducer[Msg, Msg],
) (stream.Processor[Msg, Msg], node.Reset) {
	return node.Reduce[Msg](n)
}

func (t typed[Msg]) Reduce(
	n node.Reducer[Msg, Msg],
) stream.Processor[Msg, Msg] {
	res, _ := t.ReduceWithReset(n)
	return res
}

func (t typed[Msg]) ReduceFromWithReset(
	n node.Reducer[Msg, Msg], init Msg,
) (stream.Processor[Msg, Msg], node.Reset) {
	return node.ReduceFrom[Msg](n, init)
}

func (t typed[Msg]) ReduceFrom(
	n node.Reducer[Msg, Msg], init Msg,
) stream.Processor[Msg, Msg] {
	res, _ := t.ReduceFromWithReset(n, init)
	return res
}

func (t typed[Msg]) Subprocess(
	p ...stream.Processor[Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Subprocess[Msg](p...)
}

func (t typed[Msg]) TopicSource(
	top topic.Topic[Msg],
) stream.Processor[Msg, Msg] {
	fn := node.TopicSource[Msg](top)
	return func(_ Msg, rep stream.Reporter[Msg]) {
		fn(nil, rep)
	}
}

func (t typed[Msg]) TopicSink(
	top topic.Topic[Msg],
) stream.Processor[Msg, Msg] {
	return node.TopicSink[Msg](top)
}
