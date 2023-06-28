package build

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"

	internal "github.com/caravan/streaming/internal/stream"
)

type builder[Msg any] struct {
	prev   []*builder[Msg]
	build  Deferred[Msg]
	source stream.Processor[stream.Source, Msg]
}

func makeInitial[Msg any](
	source stream.Processor[stream.Source, Msg],
) *builder[Msg] {
	return &builder[Msg]{
		prev:   []*builder[Msg]{},
		source: source,
	}
}

// Source initiates a new Builder, with its messages originating in the provided
// SourceProcessor
func Source[Msg any](p stream.Processor[stream.Source, Msg]) Builder[Msg] {
	return makeInitial[Msg](p)
}

// TopicConsumer initiates a new Builder, with its messages originating in the
// provided Topic
func TopicConsumer[Msg any](t topic.Topic[Msg]) Builder[Msg] {
	c := node.TopicConsumer(t)
	return Source(c)
}

// Merge initiates a new Builder, with its messages originating from the
// provided Builders
func Merge[Msg any](builders ...Builder[Msg]) Builder[Msg] {
	p := buildProcessors(builders...)
	m := node.Merge(p...)
	return makeInitial[Msg](m)
}

// Join initiates a new Builder, with its messages originating from the
// provided Builders, filtered by its node.BinaryPredicate, and joined by its
// node.BinaryOperator
func Join[Msg any](
	l Builder[Msg], r Builder[Msg],
	pred node.BinaryPredicate[Msg, Msg],
	joiner node.BinaryOperator[Msg, Msg, Msg],
) Builder[Msg] {
	p := buildProcessors(l, r)
	j := node.Join(p[0], p[1], pred, joiner)
	return makeInitial[Msg](j)
}

func (b *builder[Msg]) Merge(builder ...Builder[Msg]) Builder[Msg] {
	all := append([]Builder[Msg]{b}, builder...)
	return Merge(all...)
}

func (b *builder[Msg]) Join(
	r Builder[Msg], pred node.BinaryPredicate[Msg, Msg],
	joiner node.BinaryOperator[Msg, Msg, Msg],
) Builder[Msg] {
	return Join[Msg](b, r, pred, joiner)
}

func (b *builder[Msg]) Filter(pred node.Predicate[Msg]) Builder[Msg] {
	f := node.Filter(pred)
	return b.processor(f)
}

func (b *builder[Msg]) Map(fn node.Mapper[Msg, Msg]) Builder[Msg] {
	m := node.Map(fn)
	return b.processor(m)
}

func (b *builder[Msg]) Reduce(fn node.Reducer[Msg, Msg]) Builder[Msg] {
	r := node.Reduce(fn)
	return b.processor(r)
}

func (b *builder[Msg]) ReduceFrom(
	fn node.Reducer[Msg, Msg], init Msg,
) Builder[Msg] {
	r := node.ReduceFrom(fn, init)
	return b.processor(r)
}

func (b *builder[Msg]) Processor(p stream.Processor[Msg, Msg]) Builder[Msg] {
	return b.processor(p)
}

func (b *builder[Msg]) Deferred(fn Deferred[Msg]) Builder[Msg] {
	return b.extend(fn)
}

func (b *builder[Msg]) TopicProducer(t topic.Topic[Msg]) Builder[Msg] {
	p := node.TopicProducer[Msg](t)
	return b.processor(p)
}

func (b *builder[Msg]) buildRest() stream.Processor[Msg, Msg] {
	in := append(b.prev, b)
	out := make([]stream.Processor[Msg, Msg], 0, len(in))
	for _, e := range in {
		if e.build == nil {
			continue
		}
		out = append(out, e.build())
	}
	return node.Subprocess[Msg](out...)
}

func (b *builder[Msg]) Build() stream.Processor[stream.Source, Msg] {
	return node.Bind(b.source, b.buildRest())
}

func (b *builder[_]) Stream() stream.Stream {
	return internal.Make(b.source, b.buildRest())
}

func (b *builder[Msg]) extend(build Deferred[Msg]) *builder[Msg] {
	res := *b
	res.prev = append(res.prev, b)
	res.build = build
	return &res
}

func (b *builder[Msg]) processor(p stream.Processor[Msg, Msg]) *builder[Msg] {
	return b.extend(func() stream.Processor[Msg, Msg] {
		return p
	})
}

func buildProcessors[Msg any](
	in ...Builder[Msg],
) []stream.Processor[stream.Source, Msg] {
	out := make([]stream.Processor[stream.Source, Msg], 0, len(in))
	for _, b := range in {
		out = append(out, b.Build())
	}
	return out
}
