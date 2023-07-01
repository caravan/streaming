package context

type (
	Context[Msg, Res any] struct {
		Done   <-chan Done
		Errors chan<- error
		In     <-chan Msg
		Out    chan<- Res
	}

	Done struct{}
)

func Make[Msg, Res any](
	done <-chan Done,
	errors chan<- error,
	in <-chan Msg,
	out chan<- Res,
) *Context[Msg, Res] {
	return &Context[Msg, Res]{done, errors, in, out}
}

func With[OldMsg, OldRes, Msg, Res any](
	c *Context[OldMsg, OldRes], in chan Msg, out chan Res,
) *Context[Msg, Res] {
	return Make(c.Done, c.Errors, in, out)
}

func WithIn[OldMsg, Res, Msg any](
	c *Context[OldMsg, Res], in chan Msg,
) *Context[Msg, Res] {
	return Make(c.Done, c.Errors, in, c.Out)
}

func WithOut[Msg, OldRes, Res any](
	c *Context[Msg, OldRes], out chan Res,
) *Context[Msg, Res] {
	return Make(c.Done, c.Errors, c.In, out)
}

func (c *Context[_, _]) IsDone() bool {
	select {
	case <-c.Done:
		return true
	default:
		return false
	}
}

func (c *Context[Msg, _]) FetchMessage() (Msg, bool) {
	select {
	case <-c.Done:
		var zero Msg
		return zero, false
	case msg := <-c.In:
		return msg, true
	}
}

func (c *Context[_, Res]) ForwardResult(res Res) bool {
	select {
	case <-c.Done:
		return false
	case c.Out <- res:
		return true
	}
}

func (c *Context[_, _]) ReportError(e error) bool {
	select {
	case <-c.Done:
		return false
	case c.Errors <- e:
		return true
	}
}
