package node

import (
	"sync"

	"github.com/caravan/essentials/event"
	"github.com/caravan/streaming/stream"
)

type (
	// BinaryPredicate is the signature for a function that can perform
	// Stream joining. Returning true will bind the Events in the Stream
	BinaryPredicate func(event.Event, event.Event) bool

	// Joiner combines the left and right Events into some combined result
	Joiner func(event.Event, event.Event) event.Event

	join struct {
		left      stream.Processor
		right     stream.Processor
		predicate BinaryPredicate
		joiner    Joiner
	}

	joinState int

	joinReporter struct {
		sync.Mutex
		*join
		forward stream.Reporter
		state   joinState
		left    event.Event
		right   event.Event
	}

	leftJoinReporter struct {
		*joinReporter
	}

	rightJoinReporter struct {
		*joinReporter
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

// Join accepts two Processors for the sake of joining their results based
// on a provided BinaryPredicate and Joiner. If the predicate fails,
// nothing is forwarded, otherwise the two processed Events are combined
// using the join function, and the result is forwarded
func Join(
	left stream.Processor, right stream.Processor,
	predicate BinaryPredicate, joiner Joiner,
) stream.SourceProcessor {
	return &join{
		left:      left,
		right:     right,
		predicate: predicate,
		joiner:    joiner,
	}
}

func (j *join) Source() {}

func (j *join) Process(e event.Event, r stream.Reporter) {
	var group sync.WaitGroup
	group.Add(2)

	br := &joinReporter{
		join:    j,
		forward: r,
	}

	go func() {
		j.left.Process(e, &leftJoinReporter{
			joinReporter: br,
		})
		group.Done()
	}()

	go func() {
		j.right.Process(e, &rightJoinReporter{
			joinReporter: br,
		})
		group.Done()
	}()

	group.Wait()
}

func (r *joinReporter) resolve() {
	if r.predicate(r.left, r.right) {
		r.forward.Result(r.joiner(r.left, r.right))
		r.state = joinForwarded
	} else {
		r.state = joinSkipped
	}
}

func (r *leftJoinReporter) Result(e event.Event) {
	r.Lock()
	defer r.Unlock()

	switch r.state {
	case joinRight:
		r.left = e
		r.resolve()
	case joinInit:
		r.left = e
		r.state = joinLeft
	}
	// default: perform debug logging
}

func (r *rightJoinReporter) Result(e event.Event) {
	r.Lock()
	defer r.Unlock()

	switch r.state {
	case joinLeft:
		r.right = e
		r.resolve()
	case joinInit:
		r.right = e
		r.state = joinRight
	}
	// default: perform debug logging
}

func (r *joinReporter) Error(e error) {
	r.Lock()
	defer r.Unlock()

	switch r.state {
	case joinInit, joinLeft, joinRight:
		r.forward.Error(e)
		r.state = joinFailed
	}
	// default: perform debug logging
}
