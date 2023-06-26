package streaming

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"

	_stream "github.com/caravan/streaming/internal/stream"
)

type (
	Typed[Msg any] interface {
		NewStream(...stream.Processor[Msg, Msg]) stream.Stream
		Bind(
			stream.Processor[Msg, Msg], stream.Processor[Msg, Msg],
		) stream.Processor[Msg, Msg]
		Filter(node.Predicate[Msg]) stream.Processor[Msg, Msg]
		ForEach(node.ForEachFunc[Msg]) stream.Processor[Msg, Msg]
		Generate(generator node.Generator[Msg]) stream.Processor[Msg, Msg]
		Join(
			left stream.Processor[Msg, Msg],
			right stream.Processor[Msg, Msg],
			predicate node.BinaryPredicate[Msg, Msg],
			joiner node.BinaryOperator[Msg, Msg, Msg],
		) stream.Processor[Msg, Msg]
		Map(node.Mapper[Msg, Msg]) stream.Processor[Msg, Msg]
		Merge(...stream.Processor[Msg, Msg]) stream.Processor[Msg, Msg]
		Reduce(node.Reducer[Msg, Msg]) stream.Processor[Msg, Msg]
		ReduceFrom(
			node.Reducer[Msg, Msg], Msg,
		) stream.Processor[Msg, Msg]
		Sink() stream.Processor[Msg, Msg]
		Subprocess(...stream.Processor[Msg, Msg]) stream.Processor[Msg, Msg]
		TableLookup(
			table.Table[Msg, Msg], table.ColumnName, table.KeySelector[Msg],
		) (stream.Processor[Msg, Msg], error)
		TableUpdater(table.Table[Msg, Msg]) stream.Processor[Msg, Msg]
		TopicConsumer(topic.Topic[Msg]) stream.Processor[Msg, Msg]
		TopicProducer(topic.Topic[Msg]) stream.Processor[Msg, Msg]
	}

	typed[Msg any] struct{}
)

func Of[Msg any]() Typed[Msg] {
	return typed[Msg]{}
}

func (t typed[Msg]) NewStream(p ...stream.Processor[Msg, Msg]) stream.Stream {
	return _stream.Make[Msg, Msg](
		node.Subprocess(p...),
	)
}

func (t typed[Msg]) Bind(
	l stream.Processor[Msg, Msg], r stream.Processor[Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Bind(l, r)
}

func (t typed[Msg]) Filter(n node.Predicate[Msg]) stream.Processor[Msg, Msg] {
	return node.Filter(n)
}

func (t typed[Msg]) ForEach(
	n node.ForEachFunc[Msg],
) stream.Processor[Msg, Msg] {
	return node.ForEach(n)
}

func (t typed[Msg]) Generate(
	gen node.Generator[Msg],
) stream.Processor[Msg, Msg] {
	return node.Generate(gen)
}

func (t typed[Msg]) Join(
	left stream.Processor[Msg, Msg],
	right stream.Processor[Msg, Msg],
	predicate node.BinaryPredicate[Msg, Msg],
	joiner node.BinaryOperator[Msg, Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Join(left, right, predicate, joiner)
}

func (t typed[Msg]) Map(n node.Mapper[Msg, Msg]) stream.Processor[Msg, Msg] {
	return node.Map(n)
}

func (t typed[Msg]) Merge(
	p ...stream.Processor[Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Merge(p...)
}

func (t typed[Msg]) Reduce(
	n node.Reducer[Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Reduce(n)
}

func (t typed[Msg]) ReduceFrom(
	n node.Reducer[Msg, Msg], init Msg,
) stream.Processor[Msg, Msg] {
	return node.ReduceFrom(n, init)
}

func (t typed[Msg]) Sink() stream.Processor[Msg, Msg] {
	return node.Sink[Msg]()
}

func (t typed[Msg]) Subprocess(
	p ...stream.Processor[Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.Subprocess(p...)
}

func (t typed[Msg]) TableLookup(
	tbl table.Table[Msg, Msg], c table.ColumnName, k table.KeySelector[Msg],
) (stream.Processor[Msg, Msg], error) {
	return node.TableLookup(tbl, c, k)
}

func (t typed[Msg]) TableUpdater(
	tbl table.Table[Msg, Msg],
) stream.Processor[Msg, Msg] {
	return node.TableUpdater(tbl)
}

func (t typed[Msg]) TopicConsumer(
	top topic.Topic[Msg],
) stream.Processor[Msg, Msg] {
	return node.Bind[Msg, any, Msg](
		node.Map(func(msg Msg) any { return msg }),
		node.TopicConsumer[Msg](top),
	)
}

func (t typed[Msg]) TopicProducer(
	top topic.Topic[Msg],
) stream.Processor[Msg, Msg] {
	return node.TopicProducer(top)
}
