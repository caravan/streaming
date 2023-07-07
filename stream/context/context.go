package context

type (
	Context[In, Out any] struct {
		Done   <-chan Done
		Errors chan<- error
		In     <-chan In
		Out    chan<- Out
	}

	Done struct{}
)

func Make[In, Out any](
	done <-chan Done,
	errors chan<- error,
	in <-chan In,
	out chan<- Out,
) *Context[In, Out] {
	return &Context[In, Out]{done, errors, in, out}
}

func With[OldIn, OldOut, In, Out any](
	c *Context[OldIn, OldOut], in chan In, out chan Out,
) *Context[In, Out] {
	return Make(c.Done, c.Errors, in, out)
}

func WithIn[OldIn, Out, In any](
	c *Context[OldIn, Out], in chan In,
) *Context[In, Out] {
	return Make(c.Done, c.Errors, in, c.Out)
}

func WithOut[In, OldOut, Out any](
	c *Context[In, OldOut], out chan Out,
) *Context[In, Out] {
	return Make(c.Done, c.Errors, c.In, out)
}

func (c *Context[In, Out]) IsDone() bool {
	select {
	case <-c.Done:
		return true
	default:
		return false
	}
}

func (c *Context[In, Out]) FetchMessage() (In, bool) {
	select {
	case <-c.Done:
		var zero In
		return zero, false
	case msg := <-c.In:
		return msg, true
	}
}

func (c *Context[In, Out]) ForwardResult(res Out) bool {
	select {
	case <-c.Done:
		return false
	case c.Out <- res:
		return true
	}
}

func (c *Context[In, Out]) ReportError(e error) bool {
	select {
	case <-c.Done:
		return false
	case c.Errors <- e:
		return true
	}
}
