package build

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"

	_stream "github.com/caravan/streaming/internal/stream"
)

type builder[Msg any] struct {
	prev  []*builder[Msg]
	build Deferred[Msg]
}

func makeInitial[Msg any]() *builder[Msg] {
	return &builder[Msg]{
		prev: []*builder[Msg]{},
	}
}

// Source initiates a new Builder, with its messages originating in the provided
// SourceProcessor
func Source[Msg any](p stream.Processor[Msg, Msg]) Builder[Msg] {
	return makeInitial[Msg]().processor(p)
}

// TopicConsumer initiates a new Builder, with its messages originating in the
// provided Topic
func TopicConsumer[Msg any](t topic.Topic[Msg]) Builder[Msg] {
	return Source(
		node.Bind(
			node.Map(func(msg Msg) any { return msg }),
			node.TopicConsumer(t),
		),
	)
}

// Merge initiates a new Builder, with its messages originating from the
// provided Builders
func Merge[Msg any](builders ...Builder[Msg]) Builder[Msg] {
	return makeInitial[Msg]().extend(
		func() (stream.Processor[Msg, Msg], error) {
			p, err := buildProcessors(builders...)
			if err != nil {
				return nil, err
			}
			return node.Merge(p...), nil
		},
	)
}

// Join initiates a new Builder, with its messages originating from the
// provided Builders, filtered by its node.BinaryPredicate, and joined by its
// node.BinaryOperator
func Join[Msg any](
	l Builder[Msg], r Builder[Msg],
	pred node.BinaryPredicate[Msg, Msg],
	joiner node.BinaryOperator[Msg, Msg, Msg],
) Builder[Msg] {
	return makeInitial[Msg]().extend(
		func() (stream.Processor[Msg, Msg], error) {
			p, err := buildProcessors(l, r)
			if err != nil {
				return nil, err
			}
			return node.Join(p[0], p[1], pred, joiner), nil
		},
	)
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
	return b.processor(node.Filter(pred))
}

func (b *builder[Msg]) Map(fn node.Mapper[Msg, Msg]) Builder[Msg] {
	return b.processor(node.Map(fn))
}

func (b *builder[Msg]) Reduce(fn node.Reducer[Msg, Msg]) Builder[Msg] {
	return b.processor(node.Reduce(fn))
}

func (b *builder[Msg]) ReduceFrom(
	fn node.Reducer[Msg, Msg], init Msg,
) Builder[Msg] {
	return b.processor(node.ReduceFrom(fn, init))
}

func (b *builder[Msg]) Processor(p stream.Processor[Msg, Msg]) Builder[Msg] {
	return b.processor(p)
}

func (b *builder[Msg]) Deferred(fn Deferred[Msg]) Builder[Msg] {
	return b.extend(fn)
}

func (b *builder[Msg]) Sink(p stream.Processor[Msg, Msg]) TerminalBuilder[Msg] {
	return b.processor(node.Bind(p, node.Sink[Msg]()))
}

func (b *builder[Msg]) TopicProducer(t topic.Topic[Msg]) Builder[Msg] {
	return b.processor(node.TopicProducer[Msg](t))
}

func (b *builder[Msg]) Build() (stream.Processor[Msg, Msg], error) {
	in := append(b.prev, b)
	out := make([]stream.Processor[Msg, Msg], 0, len(in))
	for _, e := range in {
		if e.build == nil {
			continue
		}
		eb, err := e.build()
		if err != nil {
			return nil, err
		}
		out = append(out, eb)
	}
	return node.Subprocess[Msg](out...), nil
}

func (b *builder[_]) Stream() (stream.Stream, error) {
	p, err := b.Build()
	if err != nil {
		return nil, err
	}
	return _stream.Make(p), nil
}

func (b *builder[Msg]) extend(build Deferred[Msg]) *builder[Msg] {
	res := *b
	res.prev = append(res.prev, b)
	res.build = build
	return &res
}

func (b *builder[Msg]) processor(p stream.Processor[Msg, Msg]) *builder[Msg] {
	return b.extend(func() (stream.Processor[Msg, Msg], error) {
		return p, nil
	})
}

func buildProcessors[Msg any](
	in ...Builder[Msg],
) ([]stream.Processor[Msg, Msg], error) {
	out := make([]stream.Processor[Msg, Msg], 0, len(in))
	for _, b := range in {
		eb, err := b.Build()
		if err != nil {
			return nil, err
		}
		out = append(out, eb)
	}
	return out, nil
}
