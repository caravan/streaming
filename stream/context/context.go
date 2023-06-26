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

func (c *Context[_, _]) IsDone() bool {
	select {
	case <-c.Done:
		return true
	default:
		return false
	}
}

func (c *Context[Msg, Res]) WithNewInOut(
	in chan Msg, out chan Res,
) *Context[Msg, Res] {
	res := *c
	res.In = in
	res.Out = out
	return &res
}

func (c *Context[Msg, Res]) WithNewIn(in chan Msg) *Context[Msg, Res] {
	res := *c
	res.In = in
	return &res
}

func (c *Context[Msg, Res]) WithNewOut(out chan Res) *Context[Msg, Res] {
	res := *c
	res.Out = out
	return &res
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
