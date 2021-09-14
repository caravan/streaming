package build

import (
	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/streaming/stream"
	"github.com/caravan/streaming/stream/node"
	"github.com/caravan/streaming/table"

	_stream "github.com/caravan/streaming/internal/stream"
)

type builder struct {
	prev  []*builder
	build Deferred
}

var initial = &builder{
	prev: []*builder{},
}

// Source initiates a new Builder, with its events originating in the
// provided SourceProcessor
func Source(p stream.SourceProcessor) Builder {
	return initial.processor(p)
}

// TopicSource initiates a new Builder, with its events originating in
// the provided Topic
func TopicSource(t topic.Topic) Builder {
	return Source(node.TopicSource(t))
}

// Merge initiates a new Builder, with its events originating from the
// provided Builders
func Merge(builders ...Builder) Builder {
	return initial.extend(func() (stream.Processor, error) {
		p, err := buildProcessors(builders...)
		if err != nil {
			return nil, err
		}
		return node.Merge(p...), nil
	})
}

// Join initiates a new Builder, with its events originating from the
// provided Builders, filtered by its node.BinaryPredicate, and joined by
// its node.Joiner
func Join(
	l Builder, r Builder, pred node.BinaryPredicate, joiner node.Joiner,
) Builder {
	return initial.extend(func() (stream.Processor, error) {
		p, err := buildProcessors(l, r)
		if err != nil {
			return nil, err
		}
		return node.Join(p[0], p[1], pred, joiner), nil
	})
}

func (b *builder) Merge(builder ...Builder) Builder {
	all := append([]Builder{b}, builder...)
	return Merge(all...)
}

func (b *builder) Join(
	r Builder, pred node.BinaryPredicate, joiner node.Joiner,
) Builder {
	return Join(b, r, pred, joiner)
}

func (b *builder) Filter(pred node.Predicate) Builder {
	return b.processor(node.Filter(pred))
}

func (b *builder) Map(fn node.Mapper) Builder {
	return b.processor(node.Map(fn))
}

func (b *builder) Reduce(fn node.Reducer) Builder {
	return b.processor(node.Reduce(fn))
}

func (b *builder) ReduceFrom(fn node.Reducer, e event.Event) Builder {
	return b.processor(node.ReduceFrom(fn, e))
}

func (b *builder) TableLookup(
	t table.Table, c table.ColumnName, k table.KeySelector,
) Builder {
	return b.extend(func() (stream.Processor, error) {
		return node.TableLookup(t, c, k)
	})
}

func (b *builder) Processor(p stream.Processor) Builder {
	return b.processor(p)
}

func (b *builder) ProcessorFunc(fn stream.ProcessorFunc) Builder {
	return b.Processor(fn)
}

func (b *builder) Deferred(fn Deferred) Builder {
	return b.extend(fn)
}

func (b *builder) Sink(p stream.SinkProcessor) TerminalBuilder {
	return b.processor(p)
}

func (b *builder) TopicSink(t topic.Topic) TerminalBuilder {
	return b.Sink(node.TopicSink(t))
}

func (b *builder) TableSink(t table.Table) TerminalBuilder {
	return b.Sink(node.TableSink(t))
}

func (b *builder) Build() (stream.Processor, error) {
	in := append(b.prev, b)
	out := make([]stream.Processor, 0, len(in))
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
	return node.Subprocess(out...), nil
}

func (b *builder) Stream() (stream.Stream, error) {
	p, err := b.Build()
	if err != nil {
		return nil, err
	}
	return _stream.Make(p), nil
}

func (b *builder) extend(build Deferred) *builder {
	res := *b
	res.prev = append(res.prev, b)
	res.build = build
	return &res
}

func (b *builder) processor(p stream.Processor) *builder {
	return b.extend(func() (stream.Processor, error) {
		return p, nil
	})
}

func buildProcessors(in ...Builder) ([]stream.Processor, error) {
	out := make([]stream.Processor, 0, len(in))
	for _, b := range in {
		eb, err := b.Build()
		if err != nil {
			return nil, err
		}
		out = append(out, eb)
	}
	return out, nil
}
