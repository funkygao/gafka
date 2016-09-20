package disk

// index is a memory only heap struct which is rebuilt on boot.
type index struct {
	ctx *queue
}

func newIndex(ctx *queue) *index {
	return &index{ctx: ctx}
}
