package scheduling

// Callback which is called when worker finishes processing the task
type BasicCallback interface {
	Done(result *BasicCallbackResult)
}

type BasicCallbackFunc func(result *BasicCallbackResult)

func (bcf BasicCallbackFunc) Done(result *BasicCallbackResult) {
	bcf(result)
}
