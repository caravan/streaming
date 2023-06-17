package node

import (
	"sync"

	"github.com/caravan/streaming/stream"
)

type (
	// BinaryPredicate is the signature for a function that can perform
	// Stream joining. Returning true will bind the messages in the Stream
	BinaryPredicate[Left, Right any] func(Left, Right) bool

	// BinaryOperator combines the left and right messages into some new result
	BinaryOperator[Left, Right, Res any] func(Left, Right) Res

	joinState int
)

const (
	joinInit joinState = iota
	joinLeft
	joinRight
	joinForwarded
	joinSkipped
	joinFailed
)

// Join accepts two Processors for the sake of joining their results based on a
// provided BinaryPredicate and BinaryOperator. If the predicate fails, nothing
// is forwarded, otherwise the two processed messages are combined using the join
// function, and the result is forwarded
func Join[Msg, Left, Right, Res any](
	left stream.Processor[Msg, Left],
	right stream.Processor[Msg, Right],
	predicate BinaryPredicate[Left, Right],
	joiner BinaryOperator[Left, Right, Res],
) stream.Processor[Msg, Res] {
	return func(msg Msg, rep stream.Reporter[Res]) {
		var group sync.WaitGroup
		group.Add(2)

		var leftMsg Left
		var rightMsg Right
		state := joinInit

		resolve := func() {
			if !predicate(leftMsg, rightMsg) {
				state = joinSkipped
				return
			}
			Forward(joiner(leftMsg, rightMsg), rep)
			state = joinForwarded
		}

		go func() {
			left(msg, func(res Left, err error) {
				if err != nil {
					state = joinFailed
					return
				}
				leftMsg = res
				if state == joinRight {
					resolve()
				} else {
					state = joinLeft
				}
			})
			group.Done()
		}()

		go func() {
			right(msg, func(res Right, err error) {
				if err != nil {
					state = joinFailed
					return
				}
				rightMsg = res
				if state == joinLeft {
					resolve()
				} else {
					state = joinRight
				}
			})
			group.Done()
		}()

		group.Wait()
	}
}
