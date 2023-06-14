package node

import (
	"sync"

	"github.com/caravan/streaming/stream"
)

type (
	// BinaryPredicate is the signature for a function that can perform
	// Stream joining. Returning true will bind the Events in the Stream
	BinaryPredicate[Msg any] func(Msg, Msg) bool

	// Joiner combines the left and right Events into some combined result
	Joiner[Msg any] func(Msg, Msg) Msg

	join[Msg any] struct {
		left      stream.Processor[Msg]
		right     stream.Processor[Msg]
		predicate BinaryPredicate[Msg]
		joiner    Joiner[Msg]
	}

	joinState int

	joinReporter[Msg any] struct {
		sync.Mutex
		*join[Msg]
		forward stream.Reporter[Msg]
		state   joinState
		left    Msg
		right   Msg
	}

	leftJoinReporter[Msg any] struct {
		*joinReporter[Msg]
	}

	rightJoinReporter[Msg any] struct {
		*joinReporter[Msg]
	}
)

const (
	joinInit = iota
	joinLeft
	joinRight
	joinForwarded
	joinFailed
	joinSkipped
)

// Join accepts two Processors for the sake of joining their results based on a
// provided BinaryPredicate and Joiner. If the predicate fails, nothing is
// forwarded, otherwise the two processed Events are combined using the join
// function, and the result is forwarded
func Join[Msg any](
	left stream.Processor[Msg], right stream.Processor[Msg],
	predicate BinaryPredicate[Msg], joiner Joiner[Msg],
) stream.SourceProcessor[Msg] {
	return &join[Msg]{
		left:      left,
		right:     right,
		predicate: predicate,
		joiner:    joiner,
	}
}

func (j *join[_]) Source() {}

func (j *join[Msg]) Process(m Msg, r stream.Reporter[Msg]) {
	var group sync.WaitGroup
	group.Add(2)

	br := &joinReporter[Msg]{
		join:    j,
		forward: r,
	}

	go func() {
		j.left.Process(m, &leftJoinReporter[Msg]{
			joinReporter: br,
		})
		group.Done()
	}()

	go func() {
		j.right.Process(m, &rightJoinReporter[Msg]{
			joinReporter: br,
		})
		group.Done()
	}()

	group.Wait()
}

func (r *joinReporter[_]) resolve() {
	if r.predicate(r.left, r.right) {
		r.forward.Result(r.joiner(r.left, r.right))
		r.state = joinForwarded
	} else {
		r.state = joinSkipped
	}
}

func (r *leftJoinReporter[Msg]) Result(m Msg) {
	r.Lock()
	defer r.Unlock()

	switch r.state {
	case joinRight:
		r.left = m
		r.resolve()
	case joinInit:
		r.left = m
		r.state = joinLeft
	}
	// default: perform debug logging
}

func (r *rightJoinReporter[Msg]) Result(m Msg) {
	r.Lock()
	defer r.Unlock()

	switch r.state {
	case joinLeft:
		r.right = m
		r.resolve()
	case joinInit:
		r.right = m
		r.state = joinRight
	}
	// default: perform debug logging
}

func (r *joinReporter[_]) Error(e error) {
	r.Lock()
	defer r.Unlock()

	switch r.state {
	case joinInit, joinLeft, joinRight:
		r.forward.Error(e)
		r.state = joinFailed
	}
	// default: perform debug logging
}
